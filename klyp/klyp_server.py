"""Klyp: clip a live stream while it is still live.

  python klyp_server.py            ->  http://127.0.0.1:7861   (the page is static/index.html)

The stream is recorded into a rolling buffer (streamlink -> ffmpeg, 4 second pieces, last ~10 minutes kept).
Two ways to make a clip. Both cut from the buffer and use the 9:16 layout you set (face cam on top, gameplay below):
  * chat spike: reads the live chat (chatread.py, no login). A sudden burst of messages cuts PRE s before and POST s after it.
  * manual clip: cuts the selected 10-120 seconds from the buffer when you press the button.
"""
import argparse, base64, glob, json, os, re, shutil, statistics, subprocess, sys, tempfile, threading, time, uuid
from collections import deque

import uvicorn
from fastapi import Body, FastAPI, HTTPException
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles

HERE = os.path.dirname(os.path.abspath(__file__))
BUF = os.path.join(HERE, "klyp_buffer")      # rolling recording
WORK = os.path.join(HERE, "klyp_work")       # temporary files while a clip is cut
OUT = os.path.join(HERE, "clips")            # finished 9:16 clips
STATIC = os.path.join(HERE, "static")
SEG = 4                                      # seconds per buffer piece
KEEP = 600                                   # seconds of stream kept in the buffer
PRE, POST = 25, 6                            # chat spike: seconds kept before / after the spike
SPIKE, MIN_RATE, COOLDOWN, WARMUP = 3.0, 1.5, 60, 60
OW, OH = 1440, 2560                          # output size: vertical 2K (9:16)
STREAMLINK = shutil.which("streamlink") or os.path.join(os.path.dirname(sys.executable), "streamlink")
for d in (BUF, WORK, OUT):
    os.makedirs(d, exist_ok=True)

LOCK = threading.RLock()
ST = {
    "url": "", "running": False, "rec_start": 0.0, "error": "", "notice": "",
    "face": None, "game": None, "facecam": False, "layout": None,
    "chat_on": False, "chat_rate": 0.0, "normal_rate": 0.0,
    "clips": [], "rendering": 0, "stream_name": "LiveStream", "drive_save": False,
}
REC = {"procs": [], "stop": threading.Event()}
CHAT = deque(maxlen=200000)                  # arrival time (wall clock) of every chat message
CHAT_EV = {"stop": threading.Event()}


def log(*m):
    print(time.strftime("%H:%M:%S"), *m, flush=True)


# ---------------------------------------------------------------- 1. recording into the buffer
def segments():
    """[(start_time, file)] of the buffer pieces, oldest first. The name holds the start time (local clock)."""
    out = []
    for f in glob.glob(os.path.join(BUF, "*.ts")):
        try:
            out.append((time.mktime(time.strptime(os.path.basename(f)[:-3], "%Y%m%d%H%M%S")), f))
        except ValueError:
            pass
    return sorted(out)


def start_recording(url):
    stop_all()
    shutil.rmtree(BUF, ignore_errors=True); os.makedirs(BUF, exist_ok=True)
    stop = threading.Event(); REC["stop"] = stop
    if os.path.isfile(url):                  # a local video file plays as if it were live (for testing)
        src = ["ffmpeg", "-v", "error", "-re", "-stream_loop", "-1", "-i", url, "-c", "copy", "-f", "mpegts", "pipe:1"]
    else:
        src = [STREAMLINK, "--stdout", "--hls-live-edge", "2", url, "720p,720p60,480p,best"]
    errs = open(os.path.join(HERE, "klyp.log"), "a")
    p1 = subprocess.Popen(src, stdout=subprocess.PIPE, stderr=subprocess.DEVNULL)
    p2 = subprocess.Popen(["ffmpeg", "-v", "error", "-y", "-i", "pipe:0", "-map", "0:v:0", "-map", "0:a:0?",
                           "-c", "copy", "-f", "segment", "-segment_time", str(SEG), "-strftime", "1",
                           os.path.join(BUF, "%Y%m%d%H%M%S.ts")], stdin=p1.stdout, stderr=errs)
    p1.stdout.close()
    REC["procs"] = [p1, p2]
    with LOCK:
        ST.update(url=url, running=True, rec_start=time.time(), error="", facecam=False)
    log("recording", url)
    threading.Thread(target=watch, args=(p2, stop), daemon=True).start()
    threading.Thread(target=prune, args=(stop,), daemon=True).start()


def watch(p2, stop):
    p2.wait()
    if not stop.is_set():
        with LOCK:
            ST.update(running=False, error="The stream ended or went offline. Paste the link again to restart.")
        stop_chat()
        log("recording stopped")


def prune(stop):
    while not stop.wait(5):
        cutoff = time.time() - KEEP
        for st, f in segments():
            if st < cutoff:
                try: os.remove(f)
                except OSError: pass


def stop_all():
    stop_chat()
    REC["stop"].set()
    for p in REC["procs"]:
        if p.poll() is None: p.terminate()
    REC["procs"] = []
    with LOCK:
        ST["running"] = False


# ---------------------------------------------------------------- 2. chat spikes
def chat_reader(url, ev):
    from chatread import read                # no login: the same public chat the website uses
    while not ev.is_set():
        try:
            for msg in read(url):
                if ev.is_set(): return
                with LOCK: CHAT.append((time.time(), msg))
        except Exception as e:
            if ev.is_set(): return
            log("chat reconnecting:", e); time.sleep(5)


def per_second(now, span):
    """Chat messages per second for the last `span` seconds, oldest first."""
    c = [0] * span
    with LOCK:
        for t, _ in reversed(CHAT):
            k = int(now - t)
            if k >= span: break
            if k >= 0: c[span - 1 - k] += 1
    return c


def rates(now, t0):
    sec = per_second(now, 160)
    rate = sum(sec[-8:]) / 8                                   # chat arrives in bursts: 8 second windows
    hist = sec[:-8][-int(min(152, max(8, now - t0 - 8))):]
    bins = [sum(hist[i:i + 8]) / 8 for i in range(0, len(hist) - 7, 4)]
    base = max(statistics.median(bins) if bins else 0.0, 0.2)  # the stream's normal chat speed
    warm = now - t0 >= WARMUP and len(CHAT) >= 20
    return rate, base, warm


def detector(ev, t0):
    last = -1e9
    while not ev.wait(1):
        now = time.time()
        rate, base, warm = rates(now, t0)
        with LOCK: ST.update(chat_rate=round(rate, 1), normal_rate=round(base, 1))
        if not warm: continue
        if rate >= max(SPIKE * base, MIN_RATE) and now - last > COOLDOWN:
            last = now
            queue_clip(f"chat spike ({rate:.1f}/s, normal {base:.1f}/s)", now - PRE, now + POST)


def start_chat():
    if not re.search(r"youtube\.com|youtu\.be|twitch\.tv", ST["url"]):
        raise HTTPException(400, "Chat spike clipping needs a YouTube or Twitch live link.")
    stop_chat()
    CHAT.clear()
    ev = threading.Event(); CHAT_EV["stop"] = ev
    t0 = time.time()
    threading.Thread(target=chat_reader, args=(ST["url"], ev), daemon=True).start()
    threading.Thread(target=detector, args=(ev, t0), daemon=True).start()
    with LOCK: ST["chat_on"] = True
    log("chat spike clipping on")


def stop_chat():
    CHAT_EV["stop"].set()
    with LOCK: ST["chat_on"] = False


# ---------------------------------------------------------------- 3. the 9:16 layout
def clamp(v, lo, hi):
    return max(lo, min(hi, v))


def num(v, default):
    try: return float(v)
    except (TypeError, ValueError): return default


def clean_box(b):
    if not b or len(b) != 4: return None
    x, y, w, h = [int(round(num(v, 0))) for v in b]
    if w < 8 or h < 8: return None
    return [max(0, x), max(0, y), w, h]


def clean_layout(d):
    def region(r):
        r = r or {}
        return {"zoom": clamp(num(r.get("zoom"), 1), 0.3, 3),
                "x": clamp(num(r.get("x"), 0), -1, 1),
                "y": clamp(num(r.get("y"), 0), -1, 1)}
    return {"split": clamp(num(d.get("split"), 0.45), 0.2, 0.8),
            "preset": str(d.get("preset", "split_face_top"))[:40],
            "face_size": clamp(num(d.get("face_size"), 0.28), 0.12, 0.6),
            "face_x": clamp(num(d.get("face_x"), 0.05), 0, 1-clamp(num(d.get("face_size"), 0.28), 0.12, 0.6)),
            "face_y": clamp(num(d.get("face_y"), 0.05), 0, 1-clamp(num(d.get("face_size"), 0.28), 0.12, 0.6)),
            "face": region(d.get("face")), "game": region(d.get("game")),
            "layers": [{"id": str(v.get("id", "art"))[:40], "data": str(v.get("data", ""))[:2800000],
                       "x": clamp(num(v.get("x"), 5), 0, 100), "y": clamp(num(v.get("y"), 5), 0, 100),
                       "w": clamp(num(v.get("w"), 25), 2, 100)}
                      for v in (d.get("layers", [])[:6]) if isinstance(v, dict) and str(v.get("data", "")).startswith("data:image/")]}


def region_heights(split):
    fh = int(round(OH * split / 2)) * 2          # face cam height in pixels; gameplay gets the rest
    return fh, OH - fh


def fit_crop_rect(box, r):
    """Crop the marked source box for zoom, preserving its original aspect for fit-to-frame."""
    x, y, w, h = box
    zoom = clamp(num(r.get("zoom"), 1), 0.5, 3)
    cw, ch = w / max(1.0, zoom), h / max(1.0, zoom)
    cx = x + w / 2 + r.get("x", 0) * (w - cw) / 2
    cy = y + h / 2 + r.get("y", 0) * (h - ch) / 2
    return max(0, int(round(cx - cw/2))), max(0, int(round(cy - ch/2))), max(2, int(cw)), max(2, int(ch))


def crop_rect(box, ow, oh, r):
    """Where to cut from the source for one panel of the layout (the browser preview uses the same maths).
    box = the face or gameplay box in source pixels; the output panel is ow x oh; zoom 1-3; x, y in -1..1."""
    x, y, w, h = box
    ar = ow / oh
    bw = min(w, h * ar); bh = bw / ar            # largest piece of the box with the panel's shape
    cw, ch = bw / r["zoom"], bh / r["zoom"]
    cx = x + w / 2 + r["x"] * (w - cw) / 2       # slide inside the box
    cy = y + h / 2 + r["y"] * (h - ch) / 2
    cw_i = max(2, int(round(cw)) // 2 * 2); ch_i = max(2, int(round(ch)) // 2 * 2)
    return max(0, int(round(cx - cw / 2))), max(0, int(round(cy - ch / 2))), cw_i, ch_i


# ---------------------------------------------------------------- 4. cut and render one clip
def cut(a, b, name):
    """Cut the stream between wall-clock times a and b, laid out as the 9:16 layout. Returns the file path."""
    segs = segments()
    if not segs: raise RuntimeError("nothing is recorded yet")
    idx = 0
    for i, (st, _) in enumerate(segs):
        if st <= a: idx = i                      # the piece that holds time a
    chosen = [f for st, f in segs[idx:] if st <= b]
    if not chosen: raise RuntimeError("that part of the stream is no longer in the buffer")
    off, length = max(0.0, a - segs[idx][0]), b - a
    with LOCK:
        L, face, game, facecam = ST["layout"], ST["face"], ST["game"], ST.get("facecam", False)
    if not (L and game and (face or not facecam)): raise RuntimeError("set the gameplay box and layout first")
    preset = L.get("preset", "split_face_top")
    # Fit mode deliberately preserves the whole source gameplay box and adds letterboxing
    # rather than cropping the HUD to fill the vertical frame.
    if not facecam or preset == "game_fit":
        gx, gy, gw, ghh = fit_crop_rect(game, {"zoom": 1, "x": 0, "y": 0} if num(L["game"].get("zoom"), 1) < 1 else L["game"])
        if preset == "game_fit" or not facecam:
            zoom = clamp(num(L["game"].get("zoom"), 1), 0.3, 3)
            outw, outh = int(OW * min(1, zoom)), int(OH * min(1, zoom))
            px = int((OW - outw) / 2 + L["game"].get("x", 0) * (OW - outw) / 2)
            py = int((OH - outh) / 2 + L["game"].get("y", 0) * (OH - outh) / 2)
            if zoom > 1: outw, outh, px, py = OW, OH, 0, 0
            pad_x = int((OW - outw) / 2 + L['game'].get('x', 0) * (OW - outw) / 2)
            pad_y = int((OH - outh) / 2 + L['game'].get('y', 0) * (OH - outh) / 2)
            fc = f"[0:v]crop={gw}:{ghh}:{gx}:{gy},scale={outw}:{outh}:force_original_aspect_ratio=decrease,pad={OW}:{OH}:(ow-iw)/2+{pad_x}:(oh-ih)/2+{pad_y}:black,setsar=1,format=yuv420p[v]"
        else:
            fc = f"[0:v]crop={gw}:{ghh}:{gx}:{gy},scale={OW}:{OH},setsar=1,format=yuv420p[v]"
    elif preset in ("split_game_top", "split_face_top"):
        fh, gh = region_heights(L["split"])
        fx, fy, fw, fhh = crop_rect(face, OW, fh, L["face"])
        gzoom = clamp(num(L["game"].get("zoom"), 1), 0.3, 3)
        if gzoom <= 1:
            # Zoom-out keeps the entire user-marked rectangle, never crops it to 9:16 first.
            gx, gy, gw, ghh = game
            outw, outh = max(2, int(OW*gzoom)//2*2), max(2, int(gh*gzoom)//2*2)
            px = int((OW-outw)/2 + L["game"].get("x", 0)*(OW-outw)/2)
            py = int((gh-outh)/2 + L["game"].get("y", 0)*(gh-outh)/2)
            gfilter = (f"crop={gw}:{ghh}:{gx}:{gy},scale={outw}:{outh}:force_original_aspect_ratio=decrease,"
                       f"pad={OW}:{gh}:{px}:{py}:black,setsar=1,format=yuv420p[g]")
        else:
            gx, gy, gw, ghh = crop_rect(game, OW, gh, L["game"])
            gfilter = f"crop={gw}:{ghh}:{gx}:{gy},scale={OW}:{gh},setsar=1,format=yuv420p[g]"
        # In split layouts the input is split into [a] and [b]. The game
        # branch must explicitly consume [a]; the prior graph omitted this label,
        # causing FFmpeg to report "Error parsing global options: Invalid argument".
        if preset == "split_game_top":
            fc = (f"[0:v]split=2[a][b];[a]{gfilter};"
                  f"[b]crop={fw}:{fhh}:{fx}:{fy},scale={OW}:{fh},setsar=1[f];[g][f]vstack=inputs=2,format=yuv420p[v]")
        else:
            fc = (f"[0:v]split=2[a][b];[a]crop={fw}:{fhh}:{fx}:{fy},scale={OW}:{fh},setsar=1[f];"
                  f"[b]{gfilter};[f][g]vstack=inputs=2,format=yuv420p[v]")
    else:
        # Full gameplay with a movable facecam overlay. Circle mode uses alpha masking;
        # floating mode keeps a bordered rectangular facecam.
        gzoom = clamp(num(L["game"].get("zoom"), 1), 0.3, 3)
        if gzoom <= 1:
            # Match the editor's fit-to-canvas math exactly: scale the marked game
            # box to its natural aspect ratio, then overlay it on a portrait black base.
            gx, gy, gw, ghh = game
            source_ar = gw / max(1, ghh)
            fitw = min(OW, int(OH * source_ar))
            fith = min(OH, int(OW / source_ar))
            outw = max(2, int(round(fitw * gzoom)) // 2 * 2)
            outh = max(2, int(round(fith * gzoom)) // 2 * 2)
            px = int((OW-outw)/2 + L["game"].get("x", 0)*(OW-outw)/2)
            py = int((OH-outh)/2 + L["game"].get("y", 0)*(OH-outh)/2)
            gfilter = (f"[gamebg]crop={gw}:{ghh}:{gx}:{gy},scale={outw}:{outh},setsar=1[game];"
                      f"color=c=black:s={OW}x{OH}[canvas];[canvas][game]overlay={px}:{py}:shortest=1,setsar=1[base]")
        else:
            gx, gy, gw, ghh = crop_rect(game, OW, OH, L["game"])
            gfilter = f"[gamebg]crop={gw}:{ghh}:{gx}:{gy},scale={OW}:{OH},setsar=1[base]"
        size = max(2, int(OW * L.get("face_size", .28)))
        if preset == "face_float":
            face_ar = face[2] / max(1, face[3])
            fhh = max(2, int(round(size / face_ar)))
            fx, fy, fw, fhh = crop_rect(face, size, fhh, L["face"])
            posx = int(OW * clamp(num(L.get("face_x"), .05), 0, 1-size/OW))
            posy = int(OH * clamp(num(L.get("face_y"), .05), 0, 1-fhh/OH))
        else:
            fx, fy, fw, fhh = crop_rect(face, size, size, L["face"])
            posx = int(OW * L.get("face_x", .05)); posy = int(OH * L.get("face_y", .05))
        if preset == "face_circle":
            fc = (f"[0:v]split=2[gamebg][fg];{gfilter};"
                  f"[fg]crop={fw}:{fhh}:{fx}:{fy},scale={size}:{size},format=rgba,"
                  f"geq=r='r(X,Y)':g='g(X,Y)':b='b(X,Y)':a='if(lte((X-W/2)*(X-W/2)+(Y-H/2)*(Y-H/2),(min(W,H)/2)*(min(W,H)/2)),255,0)'[face];"
                  f"[base][face]overlay={posx}:{posy},format=yuv420p[v]")
        else:
            fc = (f"[0:v]split=2[gamebg][fg];{gfilter};"
                  f"[fg]crop={fw}:{fhh}:{fx}:{fy},scale={size}:{fhh},format=rgba,drawbox=x=0:y=0:w=iw:h=ih:color=white@0.9:t=5[face];"
                  f"[base][face]overlay={posx}:{posy},format=yuv420p[v]")
    work = tempfile.mkdtemp(prefix="job_", dir=WORK)  # copies, so the buffer can keep moving
    try:
        lst = os.path.join(work, "list.txt")
        with open(lst, "w") as f:
            for i, src in enumerate(chosen):
                dst = os.path.join(work, f"{i:04d}.ts"); shutil.copy(src, dst)
                f.write(f"file '{dst}'\n")
        out = os.path.join(OUT, name + ".mp4")
        # Optional user artwork/stickers: decode saved data URLs and overlay them on the final 9:16 frame.
        with LOCK: art_layers = list((ST.get("layout") or {}).get("layers", []))
        inputs = ["ffmpeg", "-v", "error", "-y", "-f", "concat", "-safe", "0", "-i", lst]
        filter_graph = fc
        current = "v"
        input_index, overlay_index = 1, 0
        for layer in art_layers[:6]:
            data = layer.get("data", "")
            try:
                header, encoded = data.split(",", 1)
                raw = base64.b64decode(encoded[:2800000])
                if len(raw) > 2 * 1024 * 1024: continue
                art_path = os.path.join(work, f"art_{overlay_index+1}.img")
                with open(art_path, "wb") as image_file: image_file.write(raw)
                inputs += ["-loop", "1", "-i", art_path]
                n = overlay_index + 1
                next_label = f"artv{n}"
                x = int(OW * clamp(num(layer.get("x"), 5), 0, 100) / 100)
                y = int(OH * clamp(num(layer.get("y"), 5), 0, 100) / 100)
                w = int(OW * clamp(num(layer.get("w"), 25), 2, 100) / 100)
                filter_graph += f";[{input_index}:v]scale={w}:-1[art{n}];[{current}][art{n}]overlay={x}:{y}:shortest=1[{next_label}]"
                current = next_label
                input_index += 1
                overlay_index += 1
            except Exception as art_error:
                log("skipping art overlay", art_error)
        r = subprocess.run(inputs + ["-ss", f"{off:.2f}", "-t", f"{length:.2f}", "-filter_complex", filter_graph,
                            "-map", f"[{current}]", "-map", "0:a:0?", "-c:v", "libx264", "-preset", "medium", "-crf", "18",
                            "-r", "30", "-c:a", "aac", "-b:a", "192k", "-movflags", "+faststart", out],
                           capture_output=True, text=True)
        if r.returncode:
            detail = r.stderr.strip()
            log("FFmpeg render failed; command:", " ".join(inputs + ["-ss", f"{off:.2f}", "-t", f"{length:.2f}", "-filter_complex", filter_graph,
                            "-map", f"[{current}]", "-map", "0:a:0?", "-c:v", "libx264", "-preset", "medium", "-crf", "18",
                            "-r", "30", "-c:a", "aac", "-b:a", "192k", "-movflags", "+faststart", out]))
            log("FFmpeg stderr:", detail)
            lines = detail.splitlines()
            raise RuntimeError("FFmpeg render failed: " + " | ".join(lines[-4:]))
        return out
    finally:
        shutil.rmtree(work, ignore_errors=True)


def queue_clip(reason, a, b):
    with LOCK: ST["rendering"] += 1
    threading.Thread(target=render_job, args=(reason, a, b), daemon=True).start()


def render_job(reason, a, b):
    try:
        wait = b - time.time()
        if wait > 0: time.sleep(wait + SEG + 1)  # the buffer piece that holds the end of the window must be finished
        stream_slug = re.sub(r"[^A-Za-z0-9_-]+", "_", ST.get("stream_name", "LiveStream")).strip("_")[:48] or "LiveStream"
        name = f"{stream_slug}_clip_{time.strftime('%Y%m%d_%H%M%S')}_{uuid.uuid4().hex[:5]}"
        clip_path = cut(a, b, name)
        drive_path = None
        if ST.get("drive_save"):
            drive_root = "/content/drive/MyDrive/Klyp Clips"
            if os.path.isdir("/content/drive/MyDrive"):
                os.makedirs(drive_root, exist_ok=True)
                drive_path = os.path.join(drive_root, name + ".mp4")
                shutil.copy2(os.path.join(OUT, name + ".mp4"), drive_path)
            else:
                with LOCK: ST["notice"] = "Google Drive save is enabled, but Drive is not mounted. Download remains available."
        with LOCK:
            ST["clips"].insert(0, {"name": name + ".mp4", "url": f"/clips/{name}.mp4", "reason": reason,
                                   "at": time.strftime("%H:%M:%S"), "drive_saved": bool(drive_path), "drive_path": drive_path})
        log("clip ready:", name, reason)
    except Exception as e:
        log("clip failed:", e)
        with LOCK: ST["notice"] = f"A clip failed: {e}"
    finally:
        with LOCK: ST["rendering"] -= 1


# ---------------------------------------------------------------- 5. web API
def status():
    with LOCK:
        s = {k: ST[k] for k in ("url", "running", "error", "notice", "face", "game", "facecam", "layout", "chat_on",
                                "chat_rate", "normal_rate", "clips", "rendering", "stream_name", "drive_save")}
        s["rec_seconds"] = int(time.time() - ST["rec_start"]) if ST["running"] else 0
    s["ready"] = len(segments()) >= 2
    return s


def need_setup():
    with LOCK:
        ok_boxes = ST["game"] and (ST["face"] or not ST.get("facecam", False))
        ok_layout = ST["layout"] is not None
    if not ok_boxes: raise HTTPException(400, "Mark the gameplay first (and facecam if enabled).")
    if not ok_layout: raise HTTPException(400, "Set the 9:16 layout and tap Apply first.")
    if not segments(): raise HTTPException(400, "Nothing is recorded yet. Wait a few seconds.")


app = FastAPI()


@app.post("/api/start")
def api_start(body: dict = Body(...)):
    url = str(body.get("url", "")).strip()
    if not (url.startswith("http") or os.path.isfile(url)):
        raise HTTPException(400, "Paste a YouTube, Twitch or other live stream link.")
    start_recording(url)
    with LOCK: ST.update(facecam=False, face=None, game=None, layout=None, stream_name=str(body.get("stream_name", "LiveStream"))[:100], drive_save=bool(body.get("drive_save", False)))
    return status()


@app.get("/api/status")
def api_status():
    return status()


@app.get("/api/frame.jpg")
def api_frame():
    segs = segments()
    if len(segs) < 2: raise HTTPException(503, "Still buffering. Try again in a few seconds.")
    out = os.path.join(WORK, "frame.jpg")
    if os.path.exists(out): os.remove(out)
    r = subprocess.run(["ffmpeg", "-v", "error", "-y", "-i", segs[-2][1], "-frames:v", "1", "-q:v", "3", out],
                       capture_output=True)
    if r.returncode or not os.path.exists(out): raise HTTPException(503, "Could not take a screenshot yet.")
    return FileResponse(out, headers={"Cache-Control": "no-store"})


@app.post("/api/boxes")
def api_boxes(body: dict = Body(...)):
    face, game = clean_box(body.get("face")), clean_box(body.get("game"))
    facecam = bool(body.get("facecam", ST.get("facecam", False)))
    if not game or (facecam and not face): raise HTTPException(400, "Draw the gameplay box (and facecam if enabled).")
    with LOCK: ST.update(face=face, game=game, facecam=facecam)
    return status()


@app.post("/api/layout")
def api_layout(body: dict = Body(...)):
    with LOCK: ST["layout"] = clean_layout(body)
    return status()


@app.post("/api/preset/save")
def api_preset_save(body: dict = Body(...)):
    preset = clean_layout(body)
    drive_dir = "/content/drive/MyDrive/Klyp"
    target_dir = drive_dir if os.path.isdir("/content/drive/MyDrive") else HERE
    os.makedirs(target_dir, exist_ok=True)
    target = os.path.join(target_dir, "klyp_editor_preset.json")
    with open(target, "w", encoding="utf-8") as f:
        json.dump(preset, f, indent=2)
    return {"saved": True, "location": target}


@app.get("/api/preset/load")
def api_preset_load():
    candidates = ["/content/drive/MyDrive/Klyp/klyp_editor_preset.json", os.path.join(HERE, "klyp_editor_preset.json")]
    for target in candidates:
        if os.path.isfile(target):
            with open(target, encoding="utf-8") as f:
                return clean_layout(json.load(f))
    raise HTTPException(404, "No saved editor preset found yet.")



@app.post("/api/chat/start")
def api_chat_start():
    need_setup()
    start_chat()
    return status()


@app.post("/api/chat/stop")
def api_chat_stop():
    stop_chat()
    return status()


@app.post("/api/clip/last")
def api_last(body: dict = Body(...)):
    need_setup()
    seconds = int(clamp(num(body.get("seconds"), 30), 10, 120))
    now = time.time()
    queue_clip(f"last {seconds} sec", now - seconds, now)
    return status()


@app.post("/api/stop")
def api_stop():
    stop_all()
    return status()


app.mount("/clips", StaticFiles(directory=OUT), name="clips")
app.mount("/", StaticFiles(directory=STATIC, html=True), name="static")

if __name__ == "__main__":
    ap = argparse.ArgumentParser(description="Klyp live clipper")
    ap.add_argument("--host", default="127.0.0.1")
    ap.add_argument("--port", type=int, default=7861)
    a = ap.parse_args()
    print(f"Klyp is running on port {a.port}. Keep this running while you clip.", flush=True)
    uvicorn.run(app, host=a.host, port=a.port, log_level="warning")
