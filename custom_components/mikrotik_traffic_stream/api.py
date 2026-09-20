"""Asyncio client for the RouterOS binary API, limited to what this integration needs.

Protocol reference: https://manual.mikrotik.com/docs/developer-guides/api/
"""

from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator
import ssl


class RouterOsError(Exception):
    """The router answered with !trap or !fatal."""


class RouterOsApi:
    """One connection to a RouterOS device."""

    def __init__(self, host: str, port: int, use_tls: bool, verify_tls: bool) -> None:
        self._host = host
        self._port = port
        self._use_tls = use_tls
        self._verify_tls = verify_tls
        self._reader: asyncio.StreamReader | None = None
        self._writer: asyncio.StreamWriter | None = None
        self._next_tag = 1

    # -- connection ------------------------------------------------------------
    async def connect(self, timeout: float = 10) -> None:
        ssl_context = None
        if self._use_tls:
            ssl_context = ssl.create_default_context()
            if not self._verify_tls:
                ssl_context.check_hostname = False
                ssl_context.verify_mode = ssl.CERT_NONE
        self._reader, self._writer = await asyncio.wait_for(
            asyncio.open_connection(self._host, self._port, ssl=ssl_context),
            timeout,
        )

    async def close(self) -> None:
        if self._writer is None:
            return
        writer, self._writer, self._reader = self._writer, None, None
        writer.close()
        try:
            await writer.wait_closed()
        except (OSError, ssl.SSLError):
            pass

    # -- word encoding ---------------------------------------------------------
    @staticmethod
    def _encode_length(length: int) -> bytes:
        if length < 0x80:
            return bytes([length])
        if length < 0x4000:
            return (length | 0x8000).to_bytes(2, "big")
        if length < 0x200000:
            return (length | 0xC00000).to_bytes(3, "big")
        if length < 0x10000000:
            return (length | 0xE0000000).to_bytes(4, "big")
        return b"\xf0" + length.to_bytes(4, "big")

    async def _read_length(self) -> int:
        assert self._reader is not None
        first = (await self._reader.readexactly(1))[0]
        if first < 0x80:
            return first
        if first < 0xC0:
            return ((first & 0x3F) << 8) | (await self._reader.readexactly(1))[0]
        if first < 0xE0:
            return ((first & 0x1F) << 16) | int.from_bytes(await self._reader.readexactly(2), "big")
        if first < 0xF0:
            return ((first & 0x0F) << 24) | int.from_bytes(await self._reader.readexactly(3), "big")
        return int.from_bytes(await self._reader.readexactly(4), "big")

    async def _read_word(self) -> str:
        assert self._reader is not None
        length = await self._read_length()
        if length == 0:
            return ""
        return (await self._reader.readexactly(length)).decode("utf-8", errors="replace")

    # -- sentences -------------------------------------------------------------
    async def write_sentence(self, words: list[str]) -> None:
        assert self._writer is not None
        payload = b"".join(
            self._encode_length(len(encoded)) + encoded for encoded in (word.encode("utf-8") for word in words)
        )
        self._writer.write(payload + b"\x00")
        await self._writer.drain()

    async def read_sentence(self) -> tuple[str, dict[str, str], str | None]:
        """Return (reply word, attributes, tag)."""
        words: list[str] = []
        while True:
            word = await self._read_word()
            if word == "":
                break
            words.append(word)
        reply = words[0] if words else ""
        attributes: dict[str, str] = {}
        tag: str | None = None
        for word in words[1:]:
            if word.startswith(".tag="):
                tag = word[5:]
            elif word.startswith("="):
                name, _, value = word[1:].partition("=")
                attributes[name] = value
        return reply, attributes, tag

    async def talk(self, words: list[str]) -> list[dict[str, str]]:
        """Send one sentence and collect !re rows until !done."""
        await self.write_sentence(words)
        rows: list[dict[str, str]] = []
        while True:
            reply, attributes, _ = await self.read_sentence()
            if reply == "!re":
                rows.append(attributes)
            elif reply == "!done":
                return rows
            elif reply in ("!trap", "!fatal"):
                raise RouterOsError(attributes.get("message", str(attributes)))

    async def login(self, username: str, password: str) -> None:
        """RouterOS 6.43 and later plain login."""
        await self.talk(["/login", f"=name={username}", f"=password={password}"])

    async def stream(self, words: list[str]) -> AsyncIterator[dict[str, str]]:
        """Run a non-terminating command and yield each !re row.

        The command is tagged so that a /cancel sent on generator close only
        stops this command. Ends when the router sends !done for the tag.
        """
        tag = str(self._next_tag)
        self._next_tag += 1
        await self.write_sentence(words + [f".tag={tag}"])
        try:
            while True:
                reply, attributes, reply_tag = await self.read_sentence()
                if reply_tag != tag:
                    continue
                if reply == "!re":
                    yield attributes
                elif reply == "!done":
                    return
                elif reply in ("!trap", "!fatal"):
                    raise RouterOsError(attributes.get("message", str(attributes)))
        finally:
            if self._writer is not None and not self._writer.is_closing():
                try:
                    await self.write_sentence(["/cancel", f"=tag={tag}"])
                except (OSError, ssl.SSLError):
                    pass
