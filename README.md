# Python_MQTT_Broker
Python MQTT broker implementation: lightweight, event-driven server handling client connections, topic subscriptions, QoS levels, retained messages and persistence.
Minimal MQTT broker using asyncio.

# Features:
- CONNECT / CONNACK (very simplified parsing)
- SUBSCRIBE / SUBACK (single-filter simplified parsing)
- PUBLISH (QoS 0/1/2 simplified handling)
- PUBACK / PUBREC / PUBREL / PUBCOMP (basic flows)
- PINGREQ / PINGRESP, DISCONNECT
- Retained messages
- Persistent sessions: subscriptions and pending QoS2 persisted to JSON
- Per-subscription QoS stored as {"client_id": "...", "qos": n}
- Defensive normalization of loaded state to avoid malformed entries
# Limitations:
- Simplified MQTT parsing (single-byte remaining length, simplified CONNECT layout)
- No authentication, no TLS, no will messages, no session expiry, no topic aliases, etc.
- No support for MQTT 5 features like user properties, reason codes, etc.
