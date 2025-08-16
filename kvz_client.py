import zmq
import struct


class KVZClient:
    """
    Python client for the kvz Rust key/value store.
    Protocol:
      PUT: ["PUT", key, ts(8B BE), data]
      GET: ["GET", key]
    """

    def __init__(self, connect="tcp://localhost:5555"):
        ctx = zmq.Context()
        self.sock = ctx.socket(zmq.REQ)
        self.sock.connect(connect)

    def put(self, key: str, ts: int, data: bytes) -> str:
        """Store value, returns "OK", "STALE", or raises Exception."""
        ts_bytes = struct.pack(">Q", ts)  # 8 bytes big-endian
        self.sock.send_multipart([b"PUT", key.encode("utf-8"), ts_bytes, data])
        rep = self.sock.recv_multipart()
        if not rep:
            raise RuntimeError("empty reply")
        code = rep[0].decode("utf-8", errors="ignore")
        if code in ("OK", "STALE"):
            return code
        if code == "ERR":
            msg = rep[1].decode("utf-8", errors="ignore") if len(rep) > 1 else ""
            raise RuntimeError(f"PUT ERR: {msg}")
        raise RuntimeError(f"Unexpected reply: {rep}")

    def get(self, key: str):
        """
        Fetch value. Returns (timestamp:int, data:bytes) or None if not found.
        Raises Exception on error.
        """
        self.sock.send_multipart([b"GET", key.encode("utf-8")])
        rep = self.sock.recv_multipart()
        if not rep:
            raise RuntimeError("empty reply")
        code = rep[0].decode("utf-8", errors="ignore")
        if code == "MISS":
            return None
        if code == "OK":
            if len(rep) != 3:
                raise RuntimeError("malformed OK reply")
            ts = struct.unpack(">Q", rep[1])[0]
            data = rep[2]
            return ts, data
        if code == "ERR":
            msg = rep[1].decode("utf-8", errors="ignore") if len(rep) > 1 else ""
            raise RuntimeError(f"GET ERR: {msg}")
        raise RuntimeError(f"Unexpected reply: {rep}")


if __name__ == "__main__":
    import time

    client = KVZClient("tcp://localhost:5555")

    # Simple demo: put some text, then get it back
    ts = int(time.time() * 1000)
    print("PUT:", client.put("greeting", ts, b"hello from python"))
    res = client.get("greeting")
    if res is None:
        print("MISS")
    else:
        ts, data = res
        print(f"GET: ts={ts}, data={data!r}")
