"""Fall back to Google DNS when the OS resolver cannot look up Bitget hosts."""
import socket
import struct
import threading

_DNS_SERVERS = ("8.8.8.8", "1.1.1.1")
_CACHE = {}
_LOCK = threading.Lock()
_ORIG = socket.getaddrinfo


def _dns_query_a(host, server):
    host = host.strip().rstrip(".")
    labels = host.split(".")
    q = b"".join(bytes([len(p)]) + p.encode("ascii") for p in labels) + b"\x00"
    txid = 0x1234
    header = struct.pack("!HHHHHH", txid, 0x0100, 1, 0, 0, 0)
    question = q + struct.pack("!HH", 1, 1)
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    try:
        sock.settimeout(2.5)
        sock.sendto(header + question, (server, 53))
        data, _ = sock.recvfrom(512)
    finally:
        sock.close()
    if len(data) < 12:
        return []
    qdcount = struct.unpack("!H", data[4:6])[0]
    ancount = struct.unpack("!H", data[6:8])[0]
    offset = 12

    def skip_name(buf, i):
        while True:
            if i >= len(buf):
                raise ValueError("bad name")
            length = buf[i]
            if length == 0:
                return i + 1
            if length & 0xC0 == 0xC0:
                return i + 2
            i += 1 + length

    for _ in range(qdcount):
        offset = skip_name(data, offset) + 4
    ips = []
    for _ in range(ancount):
        offset = skip_name(data, offset)
        if offset + 10 > len(data):
            break
        rtype, _, _, rdlen = struct.unpack("!HHIH", data[offset:offset + 10])
        offset += 10
        rdata = data[offset:offset + rdlen]
        offset += rdlen
        if rtype == 1 and rdlen == 4:
            ips.append(socket.inet_ntoa(rdata))
    return ips


def _resolve(host):
    host_l = host.lower()
    with _LOCK:
        cached = _CACHE.get(host_l)
    if cached:
        return cached
    ips = []
    for server in _DNS_SERVERS:
        try:
            ips = _dns_query_a(host, server)
            if ips:
                break
        except Exception:
            continue
    if ips:
        with _LOCK:
            _CACHE[host_l] = ips
    return ips


def _patched_getaddrinfo(host, port, family=0, type=0, proto=0, flags=0):
    try:
        return _ORIG(host, port, family, type, proto, flags)
    except socket.gaierror:
        if not host or host[0].isdigit() or ":" in str(host):
            raise
        ips = _resolve(str(host))
        if not ips:
            raise
        results = []
        for ip in ips:
            results.extend(
                _ORIG(ip, port, family or socket.AF_INET, type or socket.SOCK_STREAM, proto, flags)
            )
        return results


def install():
    if getattr(socket.getaddrinfo, "_bitget_dns_fallback", False):
        return
    _patched_getaddrinfo._bitget_dns_fallback = True
    socket.getaddrinfo = _patched_getaddrinfo
