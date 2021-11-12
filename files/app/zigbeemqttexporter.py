#!/usr/bin/env python3
"""MQTT exporter."""

import json
import logging
import re
import signal
import sys

import paho.mqtt.client as mqtt
from prometheus_client import Counter, Gauge, start_http_server

import os

PREFIX = os.getenv("PROMETHEUS_PREFIX", "mqtt_")
TOPIC_LABEL = os.getenv("TOPIC_LABEL", "topic")
TOPIC = os.getenv("MQTT_TOPIC", "#")
IGNORED_TOPICS = os.getenv("MQTT_IGNORED_TOPICS", "").split(",")

LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO")
MQTT_ADDRESS = os.getenv("MQTT_ADDRESS", "127.0.0.1")
MQTT_PORT = int(os.getenv("MQTT_PORT", "1883"))
MQTT_KEEPALIVE = int(os.getenv("MQTT_KEEPALIVE", "60"))
MQTT_USERNAME = os.getenv("MQTT_USERNAME")
MQTT_PASSWORD = os.getenv("MQTT_PASSWORD")
PROMETHEUS_PORT = int(os.getenv("PROMETHEUS_PORT", "9004"))

logging.basicConfig(level=LOG_LEVEL)
LOG = logging.getLogger("mqtt-exporter")

STATE_VALUES = {
    "ON": 1,
    "OFF": 0,
    "TRUE": 1,
    "FALSE": 0,
}

# global variable
prom_metrics = {}  # pylint: disable=C0103
prom_msg_counter = Counter(
    f"{PREFIX}message_total", "Counter of received messages", [TOPIC_LABEL]
)


def subscribe(client, userdata, flags, connection_result):  # pylint: disable=W0613
    """Subscribe to mqtt events (callback)."""
    LOG.info('listening to "%s"', TOPIC)
    client.subscribe(TOPIC)


def _parse_metrics(data, topic, prefix=""):
    """Attempt to parse a set of metrics.

    Note when `data` contains nested metrics this function will be called recursivley.
    """
    for metric, value in data.items():
        # when value is a dict recursivley call _parse_metrics to handle these messages
        if isinstance(value, dict):
            LOG.debug("parsing dict %s: %s", metric, value)
            _parse_metrics(value, topic, f"{prefix}{metric}_")
            continue

        try:
            metric_value = _parse_metric(value)
        except ValueError as err:
            LOG.debug("Failed to convert %s: %s", metric, err)
            continue

        # create metric if does not exist
        prom_metric_name = f"{PREFIX}{prefix}{metric}".replace(".", "").replace(" ", "_")
        prom_metric_name = re.sub(r"\((.*?)\)", "", prom_metric_name)
        if not prom_metrics.get(prom_metric_name):
            prom_metrics[prom_metric_name] = Gauge(
                prom_metric_name, "metric generated from MQTT message.", [TOPIC_LABEL]
            )
            LOG.info("creating prometheus metric: %s", prom_metric_name)

        # expose the metric to prometheus
        prom_metrics[prom_metric_name].labels(**{TOPIC_LABEL: topic}).set(metric_value)
        LOG.debug("new value for %s: %s", prom_metric_name, metric_value)


def _parse_metric(data):
    """Attempt to parse the value and extract a number out of it.

    Note that `data` is untrusted input at this point.

    Raise ValueError is the data can't be parsed.
    """
    if isinstance(data, (int, float)):
        return data

    if isinstance(data, bytes):
        data = data.decode()

    if isinstance(data, str):
        data = data.upper()

        # Handling of switch data where their state is reported as ON/OFF
        if data in STATE_VALUES:
            return STATE_VALUES[data]

        # Last ditch effort, we got a string, let's try to cast it
        return float(data)

    # We were not able to extract anything, let's bubble it up.
    raise ValueError(f"Can't parse '{data}' to a number.")


def _normalize_shelly_msg(topic, payload):
    """Normalize message from Shelly sensors to classic topic payload format.

    Shelly integrated topic and payload differently:
    * topic: shellies/room/sensor/temperature
    * payload: 20.00
    """
    info = topic.split("/")
    try:
        topic = f"{info[0]}/{info[1]}"
        payload_dict = {
            info[-1]: payload.decode()
        }  # usutally the last element is the type of sensor
        payload = json.dumps(payload_dict)
    except IndexError:
        pass

    return topic, payload


def _parse_message(topic, payload):
    """Parse topic and payload to have exposable information."""
    # Shelly sensors support
    LOG.debug('=============================================================')
    LOG.debug('=============================================================')
    LOG.debug('=============================================================')
    LOG.debug('Topic "%s"', topic)
    if "shellies" in topic:
        topic, payload = _normalize_shelly_msg(topic, payload)

    topic = topic.replace("/", "_")
    # parse MQTT topic and payload
    try:
        payload = json.loads(payload)        
    except json.JSONDecodeError:
        LOG.debug('failed to parse as JSON: "%s"', payload)
        return None, None
    except UnicodeDecodeError:
        LOG.debug('encountered undecodable payload: "%s"', payload)
        return None, None

    # handle payload having single values and
    if not isinstance(payload, dict):
        info = topic.split("/")            
        payload_dict = {            
            info[-1]: payload
        }

        return topic, json.dumps(payload_dict)

    return topic, payload


def expose_metrics(client, userdata, msg):  # pylint: disable=W0613
    """Expose metrics to prometheus when a message has been published (callback)."""
    
    LOG.debug('Topic "%s"', msg.topic)
    if msg.topic in IGNORED_TOPICS:
        LOG.debug('Topic "%s" was ignored', msg.topic)
        return

    topic, payload = _parse_message(msg.topic, msg.payload)

    if not topic or not payload:
        return

    _parse_metrics(payload, topic)

    # increment received message counter
    prom_msg_counter.labels(**{TOPIC_LABEL: topic}).inc()


def main():
    """Start the exporter."""
    client = mqtt.Client()

    def stop_request(signum, frame):
        """Stop handler for SIGTERM and SIGINT.

        Keyword arguments:
        signum -- signal number
        frame -- None or a frame object. Represents execution frames
        """
        LOG.warning("Stopping MQTT exporter")
        LOG.debug("SIGNAL: %s, FRAME: %s", signum, frame)
        client.disconnect()
        sys.exit(0)

    signal.signal(signal.SIGTERM, stop_request)
    signal.signal(signal.SIGINT, stop_request)

    # start prometheus server
    start_http_server(PROMETHEUS_PORT)

    # define mqtt client
    client.on_connect = subscribe
    client.on_message = expose_metrics

    # start the connection and the loop
    if MQTT_USERNAME and MQTT_PASSWORD:
        client.username_pw_set(MQTT_USERNAME, MQTT_PASSWORD)
    client.connect(MQTT_ADDRESS, MQTT_PORT, MQTT_KEEPALIVE)
    client.loop_forever()


if __name__ == "__main__":
    main()
