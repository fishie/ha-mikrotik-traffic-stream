# MikroTik Traffic Stream

Home Assistant custom integration that keeps one connection to the RouterOS
binary API open and runs `/interface/monitor-traffic` without `once` so the
router streams a sample about once a second. Samples are aggregated over a
configurable reporting interval and pushed into sensors. No polling.

Sensors per interface: RX and TX, each as average, min and max over the
reporting interval (default 5 seconds, changeable in the integration options).
Values are bits per second, displayed as Mbit/s. With a 1 second interval the
three statistics are equal since each report holds one sample.

## Install

Copy `custom_components/mikrotik_traffic_stream` into the `custom_components`
folder of your Home Assistant configuration directory and restart Home
Assistant. Then add the integration from Settings, Devices and services.

## Router side

The `api` or `api-ssl` service must be enabled under IP, Services. The user
needs the `api` and `read` policies.

## Recorder

Each sensor changes once per reporting interval. If the database grows more than you
like, exclude them:

```yaml
recorder:
  exclude:
    entity_globs:
      - sensor.*_rx_*
      - sensor.*_tx_*
```
