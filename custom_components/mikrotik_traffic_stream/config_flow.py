"""Config flow for MikroTik Traffic Stream."""

from __future__ import annotations

import asyncio
import logging
from typing import Any

import voluptuous as vol

from homeassistant.config_entries import ConfigEntry, ConfigFlow, ConfigFlowResult, OptionsFlow
from homeassistant.const import CONF_HOST, CONF_PASSWORD, CONF_PORT, CONF_USERNAME
from homeassistant.core import callback

from .api import RouterOsApi, RouterOsError
from .const import (
    CONF_INTERFACES,
    CONF_INTERVAL,
    CONF_USE_TLS,
    CONF_VERIFY_TLS,
    DEFAULT_INTERVAL,
    DEFAULT_PORT_PLAIN,
    DEFAULT_PORT_TLS,
    DOMAIN,
    MAX_INTERVAL,
    MIN_INTERVAL,
)

INTERVAL_VALIDATOR = vol.All(vol.Coerce(int), vol.Range(min=MIN_INTERVAL, max=MAX_INTERVAL))

_LOGGER = logging.getLogger(__name__)

SCHEMA = vol.Schema(
    {
        vol.Required(CONF_HOST): str,
        vol.Optional(CONF_PORT): int,
        vol.Required(CONF_USE_TLS, default=True): bool,
        vol.Required(CONF_VERIFY_TLS, default=False): bool,
        vol.Required(CONF_USERNAME): str,
        vol.Required(CONF_PASSWORD): str,
        vol.Required(CONF_INTERFACES): str,
        vol.Required(CONF_INTERVAL, default=DEFAULT_INTERVAL): INTERVAL_VALIDATOR,
    }
)


async def _validate(data: dict[str, Any]) -> list[str]:
    """Connect, log in, and take one monitor-traffic sample. Returns interface names seen."""
    api = RouterOsApi(data[CONF_HOST], data[CONF_PORT], data[CONF_USE_TLS], data[CONF_VERIFY_TLS])
    try:
        await api.connect()
        await api.login(data[CONF_USERNAME], data[CONF_PASSWORD])
        rows = await asyncio.wait_for(
            api.talk(["/interface/monitor-traffic", f"=interface={','.join(data[CONF_INTERFACES])}", "=once="]),
            10,
        )
    finally:
        await api.close()
    return [row["name"] for row in rows if "name" in row]


class TrafficStreamConfigFlow(ConfigFlow, domain=DOMAIN):
    VERSION = 1

    @staticmethod
    @callback
    def async_get_options_flow(config_entry: ConfigEntry) -> TrafficStreamOptionsFlow:
        return TrafficStreamOptionsFlow()

    async def async_step_user(self, user_input: dict[str, Any] | None = None) -> ConfigFlowResult:
        errors: dict[str, str] = {}
        if user_input is not None:
            data = dict(user_input)
            data[CONF_INTERFACES] = [name.strip() for name in data[CONF_INTERFACES].split(",") if name.strip()]
            data.setdefault(CONF_PORT, DEFAULT_PORT_TLS if data[CONF_USE_TLS] else DEFAULT_PORT_PLAIN)
            if not data[CONF_INTERFACES]:
                errors[CONF_INTERFACES] = "no_interfaces"
            else:
                try:
                    seen = await _validate(data)
                except RouterOsError as err:
                    message = str(err).lower()
                    if "login" in message or "user" in message or "password" in message:
                        errors["base"] = "invalid_auth"
                    else:
                        errors["base"] = "router_error"
                        _LOGGER.error("Router rejected monitor-traffic: %s", err)
                except (OSError, asyncio.TimeoutError, asyncio.IncompleteReadError) as err:
                    _LOGGER.error("Cannot connect to %s: %s", data[CONF_HOST], err)
                    errors["base"] = "cannot_connect"
                else:
                    missing = [name for name in data[CONF_INTERFACES] if name not in seen]
                    if missing:
                        errors[CONF_INTERFACES] = "unknown_interface"
                        _LOGGER.error("Router did not report %s, only %s", missing, seen)
            if not errors:
                await self.async_set_unique_id(f"{data[CONF_HOST]}:{data[CONF_PORT]}")
                self._abort_if_unique_id_configured()
                return self.async_create_entry(title=data[CONF_HOST], data=data)

        return self.async_show_form(
            step_id="user",
            data_schema=self.add_suggested_values_to_schema(SCHEMA, user_input),
            errors=errors,
        )


class TrafficStreamOptionsFlow(OptionsFlow):
    """Change the reporting interval after setup."""

    async def async_step_init(self, user_input: dict[str, Any] | None = None) -> ConfigFlowResult:
        if user_input is not None:
            return self.async_create_entry(data=user_input)
        current = self.config_entry.options.get(
            CONF_INTERVAL, self.config_entry.data.get(CONF_INTERVAL, DEFAULT_INTERVAL)
        )
        return self.async_show_form(
            step_id="init",
            data_schema=vol.Schema({vol.Required(CONF_INTERVAL, default=current): INTERVAL_VALIDATOR}),
        )
