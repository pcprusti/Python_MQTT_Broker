
"""
Minimal MQTT broker using asyncio.
Features:
- CONNECT / CONNACK (very simplified parsing)
- SUBSCRIBE / SUBACK (single-filter simplified parsing)
- PUBLISH (QoS 0/1/2 simplified handling)
- PUBACK / PUBREC / PUBREL / PUBCOMP (basic flows)
- PINGREQ / PINGRESP, DISCONNECT
- Retained messages
- Persistent sessions: subscriptions and pending QoS2 persisted to JSON
- Per-subscription QoS stored as {"client_id": "...", "qos": n}
- Defensive normalization of loaded state to avoid malformed entries
Limitations:
- Simplified MQTT parsing (single-byte remaining length, simplified CONNECT layout)
- No authentication, no TLS, no will messages, no session expiry, no topic aliases, etc.
- No support for MQTT 5 features like user properties, reason codes, etc.
"""

import asyncio
import struct
import json
import os
import uuid
from typing import Dict, List, Tuple, Any

DEFAULT_HOST = "127.0.0.1"
DEFAULT_PORT = 1883


class MQTTBroker:
    def __init__(self,
                 host: str = DEFAULT_HOST,
                 port: int = DEFAULT_PORT,
                 retained_file: str = "retained.json",
                 subs_file: str = "subs.json",
                 pending_file: str = "pending_qos2.json"):
        self.host = host
        self.port = port

        # Runtime state
        self.clients: Dict[str, asyncio.StreamWriter] = {}  # client_id -> writer
        self.packet_ids: Dict[str, int] = {}               # client_id -> last packet id

        # Persistent/serializable state
        # subscriptions: filter -> list of {"client_id": str, "qos": int}
        self.subscriptions: Dict[str, List[Dict[str, Any]]] = {}
        self.retained: Dict[str, str] = {}                 # topic -> message
        # pending QoS maps
        self.pending_qos1: Dict[Tuple[str, int], Tuple[str, str]] = {}  # (client_id, pid) -> (topic, message)
        self.pending_qos2: Dict[Tuple[str, int], Tuple[str, str]] = {}  # (client_id, pid) -> (topic, message)

        # Files
        self.retained_file = retained_file
        self.subs_file = subs_file
        self.pending_file = pending_file

        # Load persisted state and normalize
        self.load_state()

    # ---------------- Persistence ----------------
    def save_state(self) -> None:
        """Persist retained messages, subscriptions, and pending QoS2 to disk."""
        # Ensure subscriptions are serializable: list of dicts with client_id and qos
        subs_serializable = {}
        for flt, entries in self.subscriptions.items():
            normalized = []
            for e in entries:
                if isinstance(e, dict) and "client_id" in e:
                    normalized.append({
                        "client_id": str(e["client_id"]),
                        "qos": int(e.get("qos", 0))
                    })
            subs_serializable[flt] = normalized

        with open(self.retained_file, "w", encoding="utf-8") as f:
            json.dump(self.retained, f, ensure_ascii=False, indent=2)

        with open(self.subs_file, "w", encoding="utf-8") as f:
            json.dump(subs_serializable, f, ensure_ascii=False, indent=2)

        # pending_qos2: convert tuple keys to "client:pid" strings
        pending_serializable = {f"{cid}:{pid}": (topic, message)
                                for (cid, pid), (topic, message) in self.pending_qos2.items()}
        with open(self.pending_file, "w", encoding="utf-8") as f:
            json.dump(pending_serializable, f, ensure_ascii=False, indent=2)

    def load_state(self) -> None:
        """Load persisted state and normalize subscriptions to expected shape."""
        # Retained
        if os.path.exists(self.retained_file):
            try:
                with open(self.retained_file, "r", encoding="utf-8") as f:
                    data = json.load(f)
                    if isinstance(data, dict):
                        self.retained = {str(k): str(v) for k, v in data.items()}
            except Exception:
                self.retained = {}

        # Subscriptions: normalize to {filter: [{"client_id":..., "qos":...}, ...]}
        self.subscriptions = {}
        if os.path.exists(self.subs_file):
            try:
                with open(self.subs_file, "r", encoding="utf-8") as f:
                    raw = json.load(f)
            except Exception:
                raw = None

            if isinstance(raw, dict):
                for flt, entries in raw.items():
                    normalized: List[Dict[str, Any]] = []
                    if isinstance(entries, list):
                        for e in entries:
                            # expected dict
                            if isinstance(e, dict) and "client_id" in e:
                                normalized.append({
                                    "client_id": str(e["client_id"]),
                                    "qos": int(e.get("qos", 0))
                                })
                            # legacy: string client_id
                            elif isinstance(e, str):
                                normalized.append({"client_id": e, "qos": 0})
                            # legacy: [client_id, qos]
                            elif isinstance(e, list) and len(e) >= 1:
                                cid = str(e[0])
                                qos = int(e[1]) if len(e) > 1 else 0
                                normalized.append({"client_id": cid, "qos": qos})
                            # ignore malformed entries
                    # if entries is a string (rare), skip
                    self.subscriptions[str(flt)] = normalized
            elif isinstance(raw, list):
                # legacy: list of filters -> create empty lists
                for flt in raw:
                    if isinstance(flt, str):
                        self.subscriptions[flt] = []
            else:
                self.subscriptions = {}

        # Pending QoS2
        self.pending_qos2 = {}
        if os.path.exists(self.pending_file):
            try:
                with open(self.pending_file, "r", encoding="utf-8") as f:
                    data = json.load(f)
                if isinstance(data, dict):
                    for key, val in data.items():
                        try:
                            cid, pid_str = key.split(":")
                            pid = int(pid_str)
                            topic, message = val
                            self.pending_qos2[(str(cid), pid)] = (str(topic), str(message))
                        except Exception:
                            continue
            except Exception:
                self.pending_qos2 = {}

        # Basic validation: ensure subscriptions values are lists
        for flt, entries in list(self.subscriptions.items()):
            if not isinstance(entries, list):
                self.subscriptions[flt] = []

    # ---------------- Packet ID ----------------
    def next_packet_id(self, client_id: str) -> int:
        current = self.packet_ids.get(client_id, 0)
        current = (current + 1) % 65536
        if current == 0:
            current = 1
        self.packet_ids[client_id] = current
        return current

    # ---------------- Topic Matching ----------------
    def match_topic(self, flt: str, topic: str) -> bool:
        """Simple MQTT wildcard matching for + and #."""
        filter_levels = flt.split('/')
        topic_levels = topic.split('/')
        for i, f in enumerate(filter_levels):
            if f == '#':
                return True
            if i >= len(topic_levels):
                return False
            if f != '+' and f != topic_levels[i]:
                return False
        return len(filter_levels) == len(topic_levels)

    # ---------------- Delivery ----------------
    async def deliver_message(self, topic: str, message: str) -> None:
        """Deliver a message to all matching subscriptions. Sends concurrently."""
        tasks = []
        # iterate over a snapshot to avoid mutation issues
        for flt, entries in list(self.subscriptions.items()):
            try:
                if not isinstance(entries, list):
                    continue
                if self.match_topic(flt, topic):
                    for entry in entries:
                        if not isinstance(entry, dict):
                            continue
                        cid = entry.get("client_id")
                        qos = int(entry.get("qos", 0)) if entry.get("qos") is not None else 0
                        if not cid:
                            continue
                        if cid not in self.clients:
                            continue
                        writer = self.clients[cid]
                        pid = self.next_packet_id(cid)

                        topic_bytes = topic.encode()
                        msg_bytes = message.encode()
                        payload = struct.pack("!H", len(topic_bytes)) + topic_bytes
                        if qos > 0:
                            payload += struct.pack("!H", pid)
                        payload += msg_bytes

                        fixed_header = (3 << 4) | (qos << 1)
                        publish_packet = bytes([fixed_header, len(payload)]) + payload

                        async def send_and_track(w: asyncio.StreamWriter, cid_local: str, pid_local: int, qos_local: int, pkt: bytes):
                            try:
                                w.write(pkt)
                                await w.drain()
                                if qos_local == 1:
                                    self.pending_qos1[(cid_local, pid_local)] = (topic, message)
                                elif qos_local == 2:
                                    self.pending_qos2[(cid_local, pid_local)] = (topic, message)
                                    # persist pending QoS2 immediately
                                    self.save_state()
                            except Exception:
                                # ignore send errors; client cleanup will remove writer later
                                pass

                        tasks.append(send_and_track(writer, cid, pid, qos, publish_packet))
            except Exception:
                continue

        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)

    # ---------------- Client Handling ----------------
    async def handle_client(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        addr = writer.get_extra_info('peername')
        print(f"Client connected: {addr}")

        client_id: str = ""
        clean_session: bool = True

        try:
            while True:
                # Simplified fixed header read (single-byte remaining length)
                fixed_header = await reader.readexactly(2)
                packet_type = fixed_header[0] >> 4
                remaining_length = fixed_header[1]
                payload = await reader.readexactly(remaining_length)

                # CONNECT
                if packet_type == 1:
                    # Very simplified parsing; ensure payload long enough
                    if len(payload) < 14:
                        # malformed CONNECT
                        break
                    connect_flags = payload[7]
                    clean_session = (connect_flags & 0x02) != 0
                    client_id_len = struct.unpack("!H", payload[12:14])[0]
                    client_id = payload[14:14 + client_id_len].decode() if client_id_len > 0 else ""
                    if not client_id:
                        client_id = str(uuid.uuid4())

                    # register writer
                    self.clients[client_id] = writer

                    # restore subscriptions and retained messages if persistent session
                    if not clean_session:
                        for flt, entries in self.subscriptions.items():
                            if not isinstance(entries, list):
                                continue
                            for entry in entries:
                                if not isinstance(entry, dict):
                                    continue
                                if entry.get("client_id") == client_id:
                                    # send retained messages that match
                                    for topic, msg in self.retained.items():
                                        if self.match_topic(flt, topic):
                                            await self.deliver_message(topic, msg)

                    # send CONNACK (Connection Accepted)
                    connack = bytes([2 << 4, 2, 0, 0])
                    writer.write(connack)
                    await writer.drain()

                # PUBLISH
                elif packet_type == 3:
                    qos = (fixed_header[0] >> 1) & 0x03
                    topic_len = struct.unpack("!H", payload[0:2])[0]
                    topic = payload[2:2 + topic_len].decode()
                    if qos > 0:
                        packet_id = struct.unpack("!H", payload[2 + topic_len:4 + topic_len])[0]
                        message = payload[4 + topic_len:].decode()
                    else:
                        packet_id = None
                        message = payload[2 + topic_len:].decode()

                    # retain flag
                    if fixed_header[0] & 0x01:
                        if message == "":
                            self.retained.pop(topic, None)
                        else:
                            self.retained[topic] = message
                        self.save_state()

                    # respond to publisher depending on QoS
                    if qos == 1 and packet_id is not None:
                        puback = bytes([4 << 4, 2]) + struct.pack("!H", packet_id)
                        writer.write(puback)
                        await writer.drain()
                    elif qos == 2 and packet_id is not None:
                        pubrec = bytes([5 << 4, 2]) + struct.pack("!H", packet_id)
                        writer.write(pubrec)
                        await writer.drain()
                        # store until PUBREL
                        self.pending_qos2[(client_id, packet_id)] = (topic, message)
                        self.save_state()

                    # deliver to subscribers
                    await self.deliver_message(topic, message)

                # SUBSCRIBE (simplified: single topic filter)
                elif packet_type == 8:
                    # minimal parsing: assume packet id (2 bytes) then one topic filter
                    if len(payload) < 5:
                        # malformed SUBSCRIBE
                        continue
                    # packet identifier at payload[0:2] (we don't use it here)
                    topic_len = struct.unpack("!H", payload[2:4])[0]
                    flt = payload[4:4 + topic_len].decode()
                    requested_qos = payload[4 + topic_len] if len(payload) > 4 + topic_len else 0

                    entry = {"client_id": client_id, "qos": int(requested_qos)}
                    # avoid duplicate subscription entries for same client and filter
                    existing = [e for e in self.subscriptions.setdefault(flt, []) if isinstance(e, dict) and e.get("client_id") == client_id]
                    if not existing:
                        self.subscriptions.setdefault(flt, []).append(entry)
                        self.save_state()

                    # SUBACK: echo back granted QoS (simplified)
                    # Build suback: packet type 9, remaining length 3, packet id (use payload[0:2]), return qos
                    packet_id_bytes = payload[0:2]
                    granted_qos = bytes([int(requested_qos)])
                    suback = bytes([9 << 4, 3]) + packet_id_bytes + granted_qos
                    writer.write(suback)
                    await writer.drain()

                    # send retained messages matching this filter
                    for topic, msg in self.retained.items():
                        if self.match_topic(flt, topic):
                            await self.deliver_message(topic, msg)

                # PUBACK
                elif packet_type == 4:
                    packet_id = struct.unpack("!H", payload[0:2])[0]
                    self.pending_qos1.pop((client_id, packet_id), None)

                # PUBREC (subscriber ack for QoS2 when we published to them)
                elif packet_type == 5:
                    packet_id = struct.unpack("!H", payload[0:2])[0]
                    # respond with PUBREL
                    pubrel = bytes([6 << 4, 2]) + struct.pack("!H", packet_id)
                    writer.write(pubrel)
                    await writer.drain()

                # PUBREL (publisher completes QoS2)
                elif packet_type == 6:
                    packet_id = struct.unpack("!H", payload[0:2])[0]
                    if (client_id, packet_id) in self.pending_qos2:
                        topic, message = self.pending_qos2.pop((client_id, packet_id))
                        # deliver to subscribers (again) and send PUBCOMP
                        await self.deliver_message(topic, message)
                        pubcomp = bytes([7 << 4, 2]) + struct.pack("!H", packet_id)
                        writer.write(pubcomp)
                        await writer.drain()
                        self.save_state()

                # PUBCOMP (final QoS2 ack)
                elif packet_type == 7:
                    packet_id = struct.unpack("!H", payload[0:2])[0]
                    self.pending_qos2.pop((client_id, packet_id), None)

                # PINGREQ
                elif packet_type == 12:
                    writer.write(bytes([13 << 4, 0]))
                    await writer.drain()

                # DISCONNECT
                elif packet_type == 14:
                    break

                else:
                    # unsupported packet types are ignored in this minimal broker
                    pass

        except asyncio.IncompleteReadError:
            # client disconnected abruptly
            pass
        except Exception as exc:
            print(f"Error handling client {addr}: {exc}")
        finally:
            # cleanup
            try:
                writer.close()
                await writer.wait_closed()
            except Exception:
                pass
            if client_id and client_id in self.clients:
                del self.clients[client_id]
            print(f"Cleaned up client {client_id} from {addr}")

    # ---------------- Server ----------------
    async def start(self) -> None:
        server = await asyncio.start_server(self.handle_client, self.host, self.port)
        print(f"MQTT broker running on {self.host}:{self.port}")
        async with server:
            try:
                await server.serve_forever()
            except asyncio.CancelledError:
                pass


def main():
    broker = MQTTBroker()
    try:
        asyncio.run(broker.start())
    except KeyboardInterrupt:
        print("Broker stopped by user")


if __name__ == "__main__":
    main()
