"""Read a live chat without logging in. Yields (text) for every new chat message.

YouTube: the same public endpoint the youtube.com chat window uses (youtubei/v1/live_chat/get_live_chat).
Twitch:  anonymous read-only IRC (nick justinfan + random number), which Twitch allows without an account.
Only standard library.
"""
import json, random, re, socket, time, urllib.request

UA = "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/130.0 Safari/537.36"


def _get(url, data=None):
    req = urllib.request.Request(url, data=json.dumps(data).encode() if data else None,
                                 headers={"User-Agent": UA, "Content-Type": "application/json", "Accept-Language": "en"})
    return urllib.request.urlopen(req, timeout=20).read().decode("utf8", "ignore")


def _yt_id(url):
    m = re.search(r"(?:v=|youtu\.be/|/live/|/shorts/)([\w-]{11})", url)
    if m: return m.group(1)
    html = _get(url)                                     # channel /live page -> current live video id
    m = re.search(r'"videoId":"([\w-]{11})"', html)
    if not m: raise RuntimeError("no live video found at " + url)
    return m.group(1)


def youtube(url):
    vid = _yt_id(url)
    html = _get(f"https://www.youtube.com/live_chat?is_popout=1&v={vid}")
    key = re.search(r'"INNERTUBE_API_KEY":"([^"]+)"', html)
    ver = re.search(r'"INNERTUBE_CLIENT_VERSION":"([^"]+)"', html)
    # the chat menu has "Top chat" then "Live chat" (every message); use "Live chat" when it is there
    reloads = re.findall(r'"reloadContinuationData":\{"continuation":"([^"]+)"', html)
    cont = re.search(r'"continuation":"([^"]+)"', html)
    if not cont and not reloads: raise RuntimeError("live chat not found (is the stream live and is chat on?)")
    api = "https://www.youtube.com/youtubei/v1/live_chat/get_live_chat?prettyPrint=false" + (f"&key={key.group(1)}" if key else "")
    ctx = {"client": {"clientName": "WEB", "clientVersion": ver.group(1) if ver else "2.20241101.00.00", "hl": "en"}}
    c, first = (reloads[1] if len(reloads) >= 2 else cont.group(1)), True
    while True:
        r = json.loads(_get(api, {"context": ctx, "continuation": c}))
        lc = r.get("continuationContents", {}).get("liveChatContinuation", {})
        if not lc: raise RuntimeError("chat ended")
        for act in lc.get("actions", []):
            item = act.get("addChatItemAction", {}).get("item", {})
            msg = item.get("liveChatTextMessageRenderer")
            if msg and not first:                        # skip the backlog that comes with the first page
                runs = msg.get("message", {}).get("runs", [])
                yield "".join(x.get("text") or x.get("emoji", {}).get("shortcuts", [":e:"])[0] for x in runs)
        first = False
        nxt = lc.get("continuations", [{}])[0]
        d = nxt.get("invalidationContinuationData") or nxt.get("timedContinuationData") or nxt.get("reloadContinuationData") or {}
        if not d.get("continuation"): raise RuntimeError("chat ended")
        c = d["continuation"]
        time.sleep(min(max(d.get("timeoutMs", 1000) / 1000, 0.5), 3))


def twitch(url):
    ch = re.search(r"twitch\.tv/([\w]+)", url).group(1).lower()
    s = socket.create_connection(("irc.chat.twitch.tv", 6667), timeout=300)
    s.sendall(f"NICK justinfan{random.randint(10000, 99999)}\r\nJOIN #{ch}\r\n".encode())
    buf = ""
    while True:
        data = s.recv(4096).decode("utf8", "ignore")
        if not data: raise RuntimeError("twitch chat closed")
        buf += data
        *lines, buf = buf.split("\r\n")
        for ln in lines:
            if ln.startswith("PING"): s.sendall(b"PONG :tmi.twitch.tv\r\n")
            elif " PRIVMSG " in ln: yield ln.split(" :", 2)[-1]


def read(url):
    return twitch(url) if "twitch.tv" in url else youtube(url)


if __name__ == "__main__":                               # quick test: python chatread.py <url>
    import sys
    t0, n = time.time(), 0
    for m in read(sys.argv[1]):
        n += 1; print(f"{time.time() - t0:5.1f}s  {m[:80]}")
        if time.time() - t0 > float(sys.argv[2] if len(sys.argv) > 2 else 20): break
    print(n, "messages")
