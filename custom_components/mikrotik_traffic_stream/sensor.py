"""Sensors for MikroTik Traffic Stream."""

from __future__ import annotations

from homeassistant.components.sensor import (
    SensorDeviceClass,
    SensorEntity,
    SensorStateClass,
)
from homeassistant.const import CONF_HOST, UnitOfDataRate
from homeassistant.core import HomeAssistant, callback
from homeassistant.helpers.device_registry import DeviceInfo
from homeassistant.helpers.entity_platform import AddEntitiesCallback

from . import TrafficConfigEntry, TrafficStreamer
from .const import DOMAIN, FIELD_RX, FIELD_TX, STATS


async def async_setup_entry(
    hass: HomeAssistant, entry: TrafficConfigEntry, async_add_entities: AddEntitiesCallback
) -> None:
    streamer = entry.runtime_data
    entities: list[TrafficRateSensor] = []
    for interface in streamer.interfaces:
        for field, label in ((FIELD_RX, "RX"), (FIELD_TX, "TX")):
            for stat in STATS:
                entities.append(TrafficRateSensor(entry, streamer, interface, field, label, stat))
    async_add_entities(entities)


class TrafficRateSensor(SensorEntity):
    """Bits per second on one interface and direction: min, max or average over the interval."""

    _attr_should_poll = False
    _attr_has_entity_name = True
    _attr_device_class = SensorDeviceClass.DATA_RATE
    _attr_state_class = SensorStateClass.MEASUREMENT
    _attr_native_unit_of_measurement = UnitOfDataRate.BITS_PER_SECOND
    _attr_suggested_unit_of_measurement = UnitOfDataRate.MEGABITS_PER_SECOND
    _attr_suggested_display_precision = 2

    def __init__(
        self,
        entry: TrafficConfigEntry,
        streamer: TrafficStreamer,
        interface: str,
        field: str,
        label: str,
        stat: str,
    ) -> None:
        self._streamer = streamer
        self._interface = interface
        self._field = field
        self._stat = stat
        self._attr_name = f"{interface} {label} {stat}"
        self._attr_unique_id = f"{entry.entry_id}_{interface}_{field}_{stat}"
        self._attr_extra_state_attributes = {"interval_seconds": streamer.interval}
        self._attr_device_info = DeviceInfo(
            identifiers={(DOMAIN, entry.entry_id)},
            name=entry.title,
            manufacturer="MikroTik",
            configuration_url=f"https://{entry.data[CONF_HOST]}",
        )

    async def async_added_to_hass(self) -> None:
        self.async_on_remove(self._streamer.add_listener(self._handle_update))

    @callback
    def _handle_update(self) -> None:
        self.async_write_ha_state()

    @property
    def available(self) -> bool:
        return self._streamer.connected and self._interface in self._streamer.latest

    @property
    def native_value(self) -> float | None:
        report = self._streamer.latest.get(self._interface, {}).get(self._field)
        return None if report is None else report[self._stat]
