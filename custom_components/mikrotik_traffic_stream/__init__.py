"""MikroTik Traffic Stream: live interface throughput over the RouterOS binary API."""

from __future__ import annotations

import asyncio
from collections.abc import Callable
import logging
from time import monotonic

from homeassistant.config_entries import ConfigEntry
from homeassistant.const import CONF_HOST, CONF_PASSWORD, CONF_PORT, CONF_USERNAME, Platform
from homeassistant.core import HomeAssistant

from .api import RouterOsApi, RouterOsError
from .const import (
    CONF_INTERFACES,
    CONF_INTERVAL,
    CONF_USE_TLS,
    CONF_VERIFY_TLS,
    DEFAULT_INTERVAL,
    FIELD_RX,
    FIELD_TX,
    RECONNECT_MAX_SECONDS,
    RECONNECT_MIN_SECONDS,
    STAT_AVERAGE,
    STAT_MAX,
    STAT_MIN,
)

_LOGGER = logging.getLogger(__name__)

PLATFORMS = [Platform.SENSOR]

type TrafficConfigEntry = ConfigEntry[TrafficStreamer]


def get_interval(entry: ConfigEntry) -> int:
    """Reporting interval in seconds, options first, then initial data, then default."""
    return int(entry.options.get(CONF_INTERVAL, entry.data.get(CONF_INTERVAL, DEFAULT_INTERVAL)))


class TrafficStreamer:
    """Keeps one monitor-traffic stream open, aggregates samples per interval, fans out to listeners.

    The router sends a sample about once a second. Samples are collected for
    `interval` seconds, then min, max and average of each direction are published
    and the buffer is cleared. With a one second interval each report holds a
    single sample, so all three statistics are equal.
    """

    def __init__(self, hass: HomeAssistant, entry: ConfigEntry) -> None:
        self._hass = hass
        self._entry = entry
        self.interfaces: list[str] = entry.data[CONF_INTERFACES]
        self.interval = get_interval(entry)
        self.connected = False
        # interface -> field -> stat -> value, as last published
        self.latest: dict[str, dict[str, dict[str, float]]] = {}
        # interface -> field -> samples collected in the current window
        self._samples: dict[str, dict[str, list[int]]] = {}
        self._window_started: float | None = None
        self._listeners: list[Callable[[], None]] = []
        self._task: asyncio.Task | None = None

    # -- listeners -------------------------------------------------------------
    def add_listener(self, callback: Callable[[], None]) -> Callable[[], None]:
        self._listeners.append(callback)

        def remove() -> None:
            self._listeners.remove(callback)

        return remove

    def _notify(self) -> None:
        for callback in list(self._listeners):
            callback()

    # -- lifecycle -------------------------------------------------------------
    def start(self) -> None:
        self._task = self._entry.async_create_background_task(
            self._hass, self._run(), name=f"{self._entry.title} traffic stream"
        )

    async def stop(self) -> None:
        if self._task is not None:
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
            self._task = None

    async def _run(self) -> None:
        backoff = RECONNECT_MIN_SECONDS
        while True:
            api = RouterOsApi(
                self._entry.data[CONF_HOST],
                self._entry.data[CONF_PORT],
                self._entry.data[CONF_USE_TLS],
                self._entry.data[CONF_VERIFY_TLS],
            )
            try:
                await api.connect()
                await api.login(self._entry.data[CONF_USERNAME], self._entry.data[CONF_PASSWORD])
                self.connected = True
                backoff = RECONNECT_MIN_SECONDS
                _LOGGER.debug("Connected to %s, streaming %s", self._entry.data[CONF_HOST], self.interfaces)
                self._notify()
                command = ["/interface/monitor-traffic", f"=interface={','.join(self.interfaces)}"]
                async for row in api.stream(command):
                    self._handle_row(row)
                _LOGGER.warning("monitor-traffic on %s ended unexpectedly, reconnecting", self._entry.data[CONF_HOST])
            except asyncio.CancelledError:
                raise
            except (OSError, asyncio.IncompleteReadError, asyncio.TimeoutError, RouterOsError) as err:
                _LOGGER.warning(
                    "Connection to %s lost (%s), retrying in %s s", self._entry.data[CONF_HOST], err, backoff
                )
            finally:
                self.connected = False
                self._samples = {}
                self._window_started = None
                self._notify()
                await api.close()
            await asyncio.sleep(backoff)
            backoff = min(backoff * 2, RECONNECT_MAX_SECONDS)

    def _handle_row(self, row: dict[str, str]) -> None:
        name = row.get("name")
        if name is None:
            return
        try:
            rx, tx = int(row[FIELD_RX]), int(row[FIELD_TX])
        except (KeyError, ValueError):
            _LOGGER.debug("Row without usable rate fields: %s", row)
            return
        now = monotonic()
        if self._window_started is None:
            self._window_started = now
        buckets = self._samples.setdefault(name, {FIELD_RX: [], FIELD_TX: []})
        buckets[FIELD_RX].append(rx)
        buckets[FIELD_TX].append(tx)
        # Rows arrive about once a second, so checking on arrival keeps the
        # window within a second of the configured interval. A one second
        # interval therefore flushes on every row, one sample per report.
        if now - self._window_started >= self.interval - 0.5:
            self._flush()

    def _flush(self) -> None:
        for name, buckets in self._samples.items():
            report = self.latest.setdefault(name, {})
            for field, values in buckets.items():
                if not values:
                    continue
                report[field] = {
                    STAT_AVERAGE: round(sum(values) / len(values)),
                    STAT_MIN: min(values),
                    STAT_MAX: max(values),
                }
        self._samples = {}
        self._window_started = None
        self._notify()


async def async_setup_entry(hass: HomeAssistant, entry: TrafficConfigEntry) -> bool:
    streamer = TrafficStreamer(hass, entry)
    entry.runtime_data = streamer
    await hass.config_entries.async_forward_entry_setups(entry, PLATFORMS)
    streamer.start()
    entry.async_on_unload(entry.add_update_listener(_async_options_updated))
    return True


async def _async_options_updated(hass: HomeAssistant, entry: TrafficConfigEntry) -> None:
    """Reload so a changed interval takes effect."""
    await hass.config_entries.async_reload(entry.entry_id)


async def async_unload_entry(hass: HomeAssistant, entry: TrafficConfigEntry) -> bool:
    await entry.runtime_data.stop()
    return await hass.config_entries.async_unload_platforms(entry, PLATFORMS)
