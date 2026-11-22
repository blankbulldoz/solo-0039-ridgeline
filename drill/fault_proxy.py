#!/usr/bin/env python3
"""Deterministic fault-injecting TCP proxy for Ridgeline failure drills.

The proxy sits between a client (producer) and the Ridgeline HTTP service and
records every wire event to a JSONL log. It supports three modes:

  * transparent : forward requests and responses unchanged
  * block       : accept the client connection and read the full request, but
                  never open the upstream connection. The request therefore
                  cannot reach the server. The connection is held open until
                  the client gives up or mode is reset.
  * hold        : forward the request to the server, read the *complete*
                  server response (proving the server committed and answered
                  on its socket), but never deliver any response byte to the
                  client. The response is held until mode is reset or the
                  client disconnects.

Mode is switched at runtime through an admin HTTP API so a drill can arm a
fault, send exactly one batch, and observe the boundary without timing
guesses. Only requests whose URL path matches the armed marker are affected;
every other request (health probes, control writes) stays transparent.

Usage:
  python3 -I drill/fault_proxy.py \
      --listen 127.0.0.1:18400 --upstream 127.0.0.1:8399 \
      --wire-log /path/wire.jsonl --admin 127.0.0.1:18401
"""
from __future__ import annotations

import argparse
import asyncio
import json
import time
from urllib.parse import urlparse


def now_ms() -> int:
    return time.time_ns() // 1_000_000


class WireLog:
    """Append-only JSONL wire log; one independent evidence stream."""

    def __init__(self, path: str) -> None:
        self.path = path
        self._lock = asyncio.Lock()

    async def event(self, kind: str, **fields: object) -> dict:
        record = {"ts_ms": now_ms(), "event": kind}
        record.update(fields)
        async with self._lock:
            with open(self.path, "a", encoding="utf-8") as fh:
                fh.write(json.dumps(record, sort_keys=True) + "\n")
        return record


def read_headers(buf: bytearray) -> tuple[dict[str, str], int] | None:
    """Return (lowercase headers, header block length) or None if incomplete."""
    marker = b"\r\n\r\n"
    idx = buf.find(marker)
    if idx < 0:
        return None
    head = bytes(buf[:idx]).split(b"\r\n")
    headers: dict[str, str] = {}
    for line in head[1:]:
        if b":" in line:
            key, value = line.split(b":", 1)
            headers[key.decode().strip().lower()] = value.decode().strip()
    return headers, idx + len(marker)


def body_length(headers: dict[str, str]) -> int | None:
    if "content-length" in headers:
        return int(headers["content-length"])
    if headers.get("transfer-encoding", "").lower() == "chunked":
        return None  # chunked bodies are not used by this drill's clients
    return 0


class FaultProxy:
    def __init__(self, args: argparse.Namespace) -> None:
        self.listen = args.listen
        self.upstream = args.upstream
        self.wire = WireLog(args.wire_log)
        # mode marker: only a request whose path contains `marker` is faulted.
        self.mode = "transparent"
        self.marker: str | None = None
        self._fired = asyncio.Event()

    # -- admin API ----------------------------------------------------------
    async def handle_admin(self, reader: asyncio.StreamReader,
                           writer: asyncio.StreamWriter) -> None:
        try:
            line = await reader.readline()
            parts = line.decode().split()
            if len(parts) < 2:
                await self._admin_reply(writer, 400, {"error": "bad request"})
                return
            method, target = parts[0], parts[1]
            url = urlparse(target)
            query = dict(
                pair.split("=", 1) for pair in url.query.split("&")
                if "=" in pair
            )
            headers_seen: list[str] = []
            while True:
                header_line = await reader.readline()
                headers_seen.append(header_line.decode().strip())
                if header_line in (b"\r\n", b"\n", b""):
                    break
            length = int(query.get("len") or 0)
            for header_line in headers_seen:
                if header_line.lower().startswith("content-length:"):
                    length = int(header_line.split(":", 1)[1].strip())
            if method == "POST" and url.path == "/arm":
                body = await reader.readexactly(length) if length else b""
                payload = json.loads(body or b"{}")
                self.mode = payload.get("mode", "transparent")
                self.marker = payload.get("marker")
                self._fired.clear()
                await self.wire.event("proxy_armed", mode=self.mode,
                                      marker=self.marker)
                await self._admin_reply(writer, 200,
                                        {"armed": self.mode, "marker": self.marker})
            elif method == "POST" and url.path == "/reset":
                self.mode = "transparent"
                self.marker = None
                await self.wire.event("proxy_reset")
                await self._admin_reply(writer, 200, {"armed": "transparent"})
            elif method == "GET" and url.path == "/state":
                await self._admin_reply(writer, 200, {
                    "mode": self.mode, "marker": self.marker,
                    "fired": self._fired.is_set()})
            elif method == "POST" and url.path == "/wait-fired":
                try:
                    await asyncio.wait_for(self._fired.wait(), timeout=10)
                    await self._admin_reply(writer, 200, {"fired": True})
                except asyncio.TimeoutError:
                    await self._admin_reply(writer, 504, {"fired": False})
            else:
                await self._admin_reply(writer, 404, {"error": "unknown route"})
        except Exception as exc:  # noqa: BLE001 - admin surface must stay up
            try:
                await self._admin_reply(writer, 500, {"error": str(exc)})
            except Exception:
                pass
        finally:
            try:
                writer.close()
                await writer.wait_closed()
            except Exception:
                pass

    async def _admin_reply(self, writer: asyncio.StreamWriter,
                           status: int, payload: dict) -> None:
        body = json.dumps(payload).encode()
        writer.write(
            f"HTTP/1.1 {status} x\r\nContent-Type: application/json\r\n"
            f"Content-Length: {len(body)}\r\nConnection: close\r\n\r\n".encode())
        writer.write(body)
        await writer.drain()

    # -- proxy plumbing -----------------------------------------------------
    async def handle_client(self, reader: asyncio.StreamReader,
                            writer: asyncio.StreamWriter) -> None:
        peer = writer.get_extra_info("peername")
        conn_id = f"{peer[0]}:{peer[1]}:{now_ms()}"
        await self.wire.event("client_connected", conn=conn_id)
        try:
            while True:
                request = await self._read_request(reader)
                if request is None:
                    break
                head, headers, body, path = request
                faulted = (self.marker is not None
                           and self.marker in path
                           and self.mode in ("block", "hold"))
                await self.wire.event(
                    "request_received", conn=conn_id, path=path,
                    bytes=len(head) + len(body), faulted=faulted,
                    mode=self.mode)
                if faulted:
                    self._fired.set()
                    if self.mode == "block":
                        await self._do_block(conn_id, reader, writer, head, body)
                        return
                    await self._do_hold(conn_id, writer, head, body, path)
                    return
                if not await self._pump_transparent(conn_id, writer, head, body):
                    return
        except (asyncio.IncompleteReadError, ConnectionResetError,
                BrokenPipeError, ConnectionError):
            pass
        finally:
            await self.wire.event("client_closed", conn=conn_id)
            try:
                writer.close()
                await writer.wait_closed()
            except Exception:
                pass

    async def _read_request(self, reader: asyncio.StreamReader):
        head = bytearray()
        parsed = None
        while parsed is None:
            chunk = await reader.read(4096)
            if not chunk:
                return None
            head.extend(chunk)
            parsed = read_headers(head)
        headers, header_len = parsed
        length = body_length(headers)
        if length is None:
            raise RuntimeError("chunked request bodies are not supported")
        body = bytes(head[header_len:])
        while len(body) < length:
            chunk = await reader.read(length - len(body))
            if not chunk:
                raise asyncio.IncompleteReadError(body, length)
            body += chunk
        request_line = bytes(head).split(b"\r\n", 1)[0].decode()
        path = request_line.split(" ")[1] if " " in request_line else ""
        return bytes(head), headers, body, path

    async def _open_upstream(self) -> tuple[asyncio.StreamReader,
                                            asyncio.StreamWriter]:
        host, port = self.upstream.split(":")
        return await asyncio.open_connection(host, int(port))

    async def _pump_transparent(self, conn_id: str,
                                client_writer: asyncio.StreamWriter,
                                head: bytes, body: bytes) -> bool:
        """Forward one request/response pair. Return False to close."""
        request_close = b"connection: close" in head.lower()
        try:
            up_reader, up_writer = await self._open_upstream()
        except OSError as exc:
            await self.wire.event("upstream_connect_failed", conn=conn_id,
                                  error=str(exc))
            return False
        try:
            up_writer.write(head)
            up_writer.write(body)
            await up_writer.drain()
            await self.wire.event("forwarded_to_upstream", conn=conn_id,
                                  bytes=len(head) + len(body))
            response = bytearray()
            parsed = None
            while parsed is None:
                chunk = await up_reader.read(4096)
                if not chunk:
                    break
                response.extend(chunk)
                parsed = read_headers(response)
            if parsed is not None:
                resp_headers, resp_header_len = parsed
                resp_len = body_length(resp_headers)
                if resp_len is None:
                    raise RuntimeError("chunked responses are not supported")
                while len(response) - resp_header_len < resp_len:
                    chunk = await up_reader.read(
                        resp_len - (len(response) - resp_header_len))
                    if not chunk:
                        raise asyncio.IncompleteReadError(response, resp_len)
                    response.extend(chunk)
            client_writer.write(bytes(response))
            await client_writer.drain()
            await self.wire.event("response_delivered", conn=conn_id,
                                  bytes=len(response))
            response_close = (b"connection: close" in
                              bytes(response).lower())
            return not (request_close or response_close)
        except (ConnectionResetError, BrokenPipeError, ConnectionError,
                asyncio.IncompleteReadError):
            return False
        finally:
            up_writer.close()
            try:
                await up_writer.wait_closed()
            except Exception:
                pass

    async def _do_block(self, conn_id: str, client_reader: asyncio.StreamReader,
                        client_writer: asyncio.StreamWriter,
                        head: bytes, body: bytes) -> None:
        # The full request is in hand; deliberately never dial upstream.
        await self.wire.event("fault_block_no_upstream", conn=conn_id,
                              request_bytes=len(head) + len(body))
        # Hold the socket open until the client aborts or resets the mode.
        try:
            while True:
                chunk = await asyncio.wait_for(client_reader.read(1),
                                               timeout=30)
                if not chunk:
                    return
        except (asyncio.TimeoutError, asyncio.IncompleteReadError,
                ConnectionResetError, ConnectionError):
            return

    async def _do_hold(self, conn_id: str,
                       client_writer: asyncio.StreamWriter,
                       head: bytes, body: bytes,
                       path: str = "") -> None:
        try:
            up_reader, up_writer = await self._open_upstream()
        except OSError as exc:
            await self.wire.event("upstream_connect_failed", conn=conn_id,
                                  error=str(exc))
            return
        up_writer.write(head)
        up_writer.write(body)
        await up_writer.drain()
        await self.wire.event("forwarded_to_upstream", conn=conn_id,
                              bytes=len(head) + len(body), path=path)
        response = bytearray()
        parsed = None
        while parsed is None:
            chunk = await up_reader.read(4096)
            if not chunk:
                break
            response.extend(chunk)
            parsed = read_headers(response)
        if parsed is None:
            await self.wire.event("fault_hold_incomplete_response",
                                  conn=conn_id, bytes=len(response))
            return
        resp_headers, resp_header_len = parsed
        resp_len = body_length(resp_headers)
        if resp_len is None:
            raise RuntimeError("chunked responses are not supported")
        while len(response) - resp_header_len < resp_len:
            chunk = await up_reader.read(resp_len -
                                         (len(response) - resp_header_len))
            if not chunk:
                raise asyncio.IncompleteReadError(response, resp_len)
            response.extend(chunk)
        status_line = bytes(response).split(b"\r\n", 1)[0].decode()
        preview = bytes(response[:256]).decode("utf-8", "replace")
        await self.wire.event("upstream_response_received", conn=conn_id,
                              status=status_line, bytes=len(response),
                              path=path, response_preview=preview)
        up_writer.close()
        try:
            await up_writer.wait_closed()
        except Exception:
            pass
        # The response exists and is logged; never write it to the client.
        await self.wire.event("fault_hold_response_withheld", conn=conn_id,
                              path=path)
        try:
            await client_writer.drain()  # flush nothing; keep socket alive
            await asyncio.sleep(30)
        except (ConnectionResetError, BrokenPipeError, ConnectionError,
                asyncio.CancelledError):
            return

    async def serve(self) -> None:
        lhost, lport = self.listen.split(":")
        server = await asyncio.start_server(self.handle_client, lhost,
                                            int(lport))
        admin_host, admin_port = args.admin.split(":")
        admin = await asyncio.start_server(self.handle_admin, admin_host,
                                           int(admin_port))
        await self.wire.event("proxy_started", listen=self.listen,
                              upstream=self.upstream, admin=args.admin)
        async with server, admin:
            await asyncio.gather(server.serve_forever(),
                                 admin.serve_forever())


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--listen", required=True)
    parser.add_argument("--upstream", required=True)
    parser.add_argument("--admin", required=True)
    parser.add_argument("--wire-log", required=True)
    return parser.parse_args()


if __name__ == "__main__":
    args = parse_args()
    asyncio.run(FaultProxy(args).serve())
