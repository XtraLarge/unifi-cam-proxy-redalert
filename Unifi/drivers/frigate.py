"""
Frigate driver for unifi-cam-proxy (redalert11 architecture).

Connects to a Frigate NVR via MQTT and translates detection events into
UniFi Protect smart-detect and analytics events.  Video is streamed by
pulling an RTSP source (e.g. go2rtc) and pushing it as extendedFLV to the
DreamMachine on port 7550 — the same mechanism the base CameraDriver uses.

Settings keys (all under the "frigate" namespace in the camera settings JSON):
    frigate.http_url        Frigate HTTP API base URL (e.g. http://10.0.0.1:5000)
    frigate.camera          Camera name inside Frigate (must match exactly)
    frigate.rtsp_url        RTSP source URL pulled by ffmpeg for video push
    frigate.mqtt_host       MQTT broker hostname / IP
    frigate.mqtt_port       MQTT broker port (default 1883)
    frigate.mqtt_username   MQTT username (optional)
    frigate.mqtt_password   MQTT password (optional)
    frigate.mqtt_prefix     MQTT topic prefix (default "frigate")
    frigate.detect_width    Detection-frame width reported to descriptor (default 1280)
    frigate.detect_height   Detection-frame height reported to descriptor (default 720)
    frigate.time_sync_ms    Timestamp offset in ms to compensate for event delay (default 0)
"""

from __future__ import annotations

import asyncio
import json
import logging
import ssl
import tempfile
import time
from enum import Enum
from pathlib import Path
from typing import Any, Callable, Coroutine, Dict, Optional
from urllib.parse import urlparse, parse_qs

import aiohttp

from Unifi.drivers.camera_driver import CameraDriver

try:
    from aiomqtt import Client as MqttClient, MqttError
    _MQTT_AVAILABLE = True
except ImportError:
    _MQTT_AVAILABLE = False


UPFLV_PREFIX = bytes.fromhex("de1916154717de19167550")


class ObjectType(str, Enum):
    PERSON = "person"
    VEHICLE = "vehicle"


_FRIGATE_LABEL_MAP: Dict[str, ObjectType] = {
    "person": ObjectType.PERSON,
    "car": ObjectType.VEHICLE,
    "truck": ObjectType.VEHICLE,
    "bus": ObjectType.VEHICLE,
    "motorcycle": ObjectType.VEHICLE,
    "vehicle": ObjectType.VEHICLE,
}


def _label_to_object_type(label: str) -> Optional[ObjectType]:
    return _FRIGATE_LABEL_MAP.get((label or "").lower())


def _coerce_dict(val: Any) -> dict:
    if isinstance(val, (bytes, bytearray)):
        try:
            val = val.decode("utf-8", "ignore")
        except Exception:
            return {}
    if isinstance(val, str):
        try:
            return json.loads(val)
        except Exception:
            return {}
    return val if isinstance(val, dict) else {}


class FrigateDriver(CameraDriver):
    """
    CameraDriver that bridges Frigate events into UniFi Protect smart-detect
    events and streams video from an RTSP source via FLV push.
    """

    def __init__(self, settings, log: logging.Logger) -> None:
        super().__init__(settings, log)

        # Event state tracking  (Frigate event ID → UniFi event ID)
        self._frigate_to_unifi: Dict[str, int] = {}
        self._active_events: Dict[int, Dict[str, Any]] = {}
        self._event_counter = 0

        # RTSP → FLV push state (videoId → {proc, writer, task})
        self._rtsp_sessions: Dict[str, Any] = {}

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    def _next_event_id(self) -> int:
        self._event_counter += 1
        return int(time.time() * 1000) * 1000 + (self._event_counter % 1000)

    def _cfg(self, key: str, default: Any = None) -> Any:
        """Read a setting from the frigate.* namespace with a fallback."""
        v = self.settings.get(f"frigate.{key}")
        return v if v is not None else default

    # ------------------------------------------------------------------
    # Snapshot (from Frigate HTTP API)
    # ------------------------------------------------------------------

    async def get_snapshot_jpeg(self, timeout_s: int = 5) -> bytes:
        http_url = self._cfg("http_url")
        camera = self._cfg("camera", "")
        if not http_url or not camera:
            return b""
        url = f"{http_url.rstrip('/')}/api/{camera}/latest.jpg"
        try:
            async with aiohttp.ClientSession() as session:
                async with session.get(url, timeout=aiohttp.ClientTimeout(total=timeout_s)) as r:
                    if r.status == 200:
                        return await r.read()
                    self.log.warning("Frigate snapshot HTTP %s from %s", r.status, url)
        except Exception as exc:
            self.log.warning("Frigate snapshot fetch failed: %s", exc)
        return b""

    # ------------------------------------------------------------------
    # Video push (RTSP → ffmpeg → extendedFLV)
    # ------------------------------------------------------------------

    async def apply_video_settings(self, cfg: Any) -> dict:
        cfg = _coerce_dict(cfg)
        video = cfg.get("video") or {}
        applied: Dict[str, Any] = {"video": {}}

        rtsp_url = self._cfg("rtsp_url")
        if not rtsp_url:
            # No RTSP source configured — stop any running push and return
            for vid in list(self._rtsp_sessions):
                await self._stop_rtsp_push(vid)
            return applied

        for vid, vcfg in video.items():
            if not isinstance(vcfg, dict):
                await self._stop_rtsp_push(vid)
                continue

            ser = vcfg.get("avSerializer") or {}
            dests = ser.get("destinations") or []
            if ser.get("type") != "extendedFlv" or not dests:
                await self._stop_rtsp_push(vid)
                continue

            u = urlparse(dests[0])
            host, port = u.hostname, u.port
            if not host or not port:
                self.log.error("Bad FLV destination for %s: %r", vid, dests[0])
                await self._stop_rtsp_push(vid)
                continue

            q = parse_qs(u.query)
            encrypted = q.get("encrypted", ["false"])[0].lower() == "true"

            ok = await self._start_rtsp_push(vid, rtsp_url, host, port, encrypted)
            applied["video"][vid] = {
                "status": "started" if ok else "failed",
                "destination": f"{'tls' if encrypted else 'tcp'}://{host}:{port}",
                "type": vcfg.get("type", "h264"),
                "avSerializer": ser,
            }

        return applied

    async def _start_rtsp_push(
        self, vid: str, rtsp_url: str, host: str, port: int, encrypted: bool
    ) -> bool:
        await self._stop_rtsp_push(vid)

        ssl_ctx: Optional[ssl.SSLContext] = None
        if encrypted:
            ssl_ctx = ssl.create_default_context()
            ssl_ctx.check_hostname = False
            ssl_ctx.verify_mode = ssl.CERT_NONE

        try:
            reader, writer = await asyncio.open_connection(host, port, ssl=ssl_ctx)
        except Exception as exc:
            self.log.error("FLV connect failed for %s: %s", vid, exc)
            return False

        try:
            writer.write(UPFLV_PREFIX)
            await writer.drain()
        except Exception as exc:
            self.log.error("uPFLV prefix write failed for %s: %s", vid, exc)
            writer.close()
            return False

        cmd = [
            "ffmpeg", "-loglevel", "error",
            "-rtsp_transport", "tcp",
            "-i", rtsp_url,
            "-c:v", "copy",
            "-ar", "32000", "-ac", "1", "-c:a", "aac", "-b:a", "32k",
            "-f", "flv", "-",
        ]
        try:
            proc = await asyncio.create_subprocess_exec(
                *cmd,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.DEVNULL,
            )
        except Exception as exc:
            self.log.error("ffmpeg start failed for %s: %s", vid, exc)
            writer.close()
            return False

        task = asyncio.create_task(self._forward_flv(vid, proc, writer))
        self._rtsp_sessions[vid] = {"proc": proc, "writer": writer, "task": task}
        self.log.info("FLV push started for %s (rtsp=%s → %s:%s)", vid, rtsp_url, host, port)
        return True

    async def _forward_flv(self, vid: str, proc: asyncio.subprocess.Process, writer) -> None:
        try:
            while True:
                chunk = await proc.stdout.read(65536)
                if not chunk:
                    break
                writer.write(chunk)
                await writer.drain()
        except asyncio.CancelledError:
            pass
        except Exception as exc:
            self.log.warning("FLV forward error for %s: %s", vid, exc)
        finally:
            try:
                writer.close()
                await writer.wait_closed()
            except Exception:
                pass
            if proc.returncode is None:
                proc.terminate()
                try:
                    await asyncio.wait_for(proc.wait(), timeout=2)
                except asyncio.TimeoutError:
                    proc.kill()

    async def _stop_rtsp_push(self, vid: str) -> None:
        sess = self._rtsp_sessions.pop(vid, None)
        if not sess:
            return
        task = sess.get("task")
        proc = sess.get("proc")
        writer = sess.get("writer")
        if task:
            task.cancel()
            try:
                await task
            except asyncio.CancelledError:
                pass
        if writer:
            try:
                writer.close()
                await writer.wait_closed()
            except Exception:
                pass
        if proc and proc.returncode is None:
            proc.terminate()
            try:
                await asyncio.wait_for(proc.wait(), timeout=2)
            except asyncio.TimeoutError:
                proc.kill()

    # ------------------------------------------------------------------
    # MQTT event loop (started by WssManager on connect)
    # ------------------------------------------------------------------

    async def start_event_loop(self) -> None:
        if not _MQTT_AVAILABLE:
            self.log.warning("aiomqtt not installed; Frigate event forwarding disabled")
            return

        mqtt_host = self._cfg("mqtt_host")
        if not mqtt_host:
            self.log.warning("frigate.mqtt_host not set; event forwarding disabled")
            return

        mqtt_port = int(self._cfg("mqtt_port", 1883))
        mqtt_user = self._cfg("mqtt_username")
        mqtt_pass = self._cfg("mqtt_password")
        prefix = self._cfg("mqtt_prefix", "frigate")
        camera = self._cfg("camera", "")

        while True:
            try:
                kwargs: Dict[str, Any] = {"port": mqtt_port}
                if mqtt_user:
                    kwargs["username"] = mqtt_user
                if mqtt_pass:
                    kwargs["password"] = mqtt_pass

                async with MqttClient(mqtt_host, **kwargs) as client:
                    self.log.info("Frigate MQTT connected to %s:%s", mqtt_host, mqtt_port)
                    await client.subscribe(f"{prefix}/events")
                    await client.subscribe(f"{prefix}/{camera}/motion")
                    await client.subscribe(f"{prefix}/{camera}/+/snapshot")

                    async for message in client.messages:
                        topic = str(message.topic)
                        try:
                            if topic == f"{prefix}/events":
                                await self._handle_detection(message.payload)
                            elif topic == f"{prefix}/{camera}/motion":
                                await self._handle_motion(message.payload)
                            elif topic.endswith("/snapshot"):
                                await self._handle_snapshot(topic, message.payload, prefix, camera)
                        except Exception:
                            self.log.exception("Error handling MQTT message on %s", topic)

            except asyncio.CancelledError:
                self.log.info("Frigate MQTT event loop cancelled")
                return
            except MqttError as exc:
                self.log.warning("Frigate MQTT error: %s — reconnecting in 10s", exc)
                await asyncio.sleep(10)
            except Exception:
                self.log.exception("Frigate MQTT unexpected error — reconnecting in 10s")
                await asyncio.sleep(10)

    # ------------------------------------------------------------------
    # MQTT message handlers
    # ------------------------------------------------------------------

    async def _handle_motion(self, payload: bytes) -> None:
        if not self.event_sender:
            return
        msg = payload.decode("utf-8", "ignore").strip() if isinstance(payload, (bytes, bytearray)) else str(payload)
        if msg == "ON":
            await self.event_sender("EventSmartMotion", {
                "clockWall": int(time.time() * 1000),
                "edgeType": "start",
                "eventType": "motion",
            })
        elif msg == "OFF":
            await self.event_sender("EventSmartMotion", {
                "clockWall": int(time.time() * 1000),
                "edgeType": "stop",
                "eventType": "motion",
            })

    async def _handle_snapshot(self, topic: str, payload: bytes, prefix: str, camera: str) -> None:
        # topic: {prefix}/{camera}/{label}/snapshot
        parts = topic.split("/")
        if len(parts) < 4:
            return
        label = parts[-2]
        # Find a matching active event by object type and store the snapshot
        obj = _label_to_object_type(label)
        if not obj:
            return
        for unifi_id, ev in self._active_events.items():
            if ev.get("object_type") == obj and ev.get("end_time") is None:
                f = tempfile.NamedTemporaryFile(delete=False, suffix=".jpg")
                f.write(payload if isinstance(payload, (bytes, bytearray)) else b"")
                f.close()
                ev["snapshot_path"] = f.name
                break

    async def _handle_detection(self, payload: bytes) -> None:
        raw = payload.decode("utf-8", "ignore") if isinstance(payload, (bytes, bytearray)) else str(payload)
        try:
            msg = json.loads(raw)
        except json.JSONDecodeError:
            self.log.warning("Frigate: cannot parse event payload")
            return

        camera = self._cfg("camera", "")
        event_camera = (msg.get("after") or {}).get("camera", "")
        if event_camera != camera:
            return

        event_type = msg.get("type")
        event_id = (msg.get("after") or {}).get("id")
        label = (msg.get("after") or {}).get("label")
        obj = _label_to_object_type(label or "")

        if not obj:
            self.log.debug("Frigate: unsupported label %r — skipping", label)
            return

        if event_type == "new":
            await self._on_event_new(event_id, msg, obj)
        elif event_type == "update":
            await self._on_event_update(event_id, msg, obj)
        elif event_type == "end":
            await self._on_event_end(event_id, msg, obj)

    async def _on_event_new(self, fid: str, msg: dict, obj: ObjectType) -> None:
        if fid in self._frigate_to_unifi:
            # Stale entry — close old event first
            old = self._frigate_to_unifi.pop(fid)
            self._active_events.pop(old, None)

        unifi_id = self._next_event_id()
        self._frigate_to_unifi[fid] = unifi_id
        self._active_events[unifi_id] = {
            "object_type": obj,
            "start_time": time.time(),
            "end_time": None,
            "snapshot_path": None,
        }

        descriptor = self._build_descriptor(msg, obj)
        frame_ms = int((msg.get("after") or {}).get("frame_time", 0) * 1000)
        await self._send_smart_detect("enter", unifi_id, obj, descriptor, frame_ms)
        self.log.info("Frigate: start %s event (fid=%s unifi=%s)", obj.value, fid, unifi_id)

    async def _on_event_update(self, fid: str, msg: dict, obj: ObjectType) -> None:
        unifi_id = self._frigate_to_unifi.get(fid)
        if unifi_id is None:
            return
        descriptor = self._build_descriptor(msg, obj)
        frame_ms = int((msg.get("after") or {}).get("frame_time", 0) * 1000)
        await self._send_smart_detect("update", unifi_id, obj, descriptor, frame_ms)

        # Refresh cached snapshot if Frigate signals one is ready
        if (msg.get("after") or {}).get("has_snapshot") and self._cfg("http_url"):
            asyncio.create_task(self._fetch_event_snapshot(fid, unifi_id))

    async def _on_event_end(self, fid: str, msg: dict, obj: ObjectType) -> None:
        unifi_id = self._frigate_to_unifi.pop(fid, None)
        if unifi_id is None:
            return
        ev = self._active_events.get(unifi_id)
        if ev:
            ev["end_time"] = time.time()

        descriptor = self._build_descriptor(msg, obj)
        end_ms = int((msg.get("after") or {}).get("end_time", 0) * 1000)
        await self._send_smart_detect("leave", unifi_id, obj, descriptor, end_ms)
        self._active_events.pop(unifi_id, None)
        self.log.info("Frigate: end %s event (fid=%s unifi=%s)", obj.value, fid, unifi_id)

    # ------------------------------------------------------------------
    # Event message builder / sender
    # ------------------------------------------------------------------

    async def _send_smart_detect(
        self,
        edge: str,
        event_id: int,
        obj: ObjectType,
        descriptor: Optional[dict],
        clock_wall: int,
    ) -> None:
        if not self.event_sender:
            return
        payload: Dict[str, Any] = {
            "clockMonotonic": int(time.monotonic() * 1000),
            "clockStream": int(time.monotonic() * 1000),
            "clockStreamRate": 1000,
            "clockWall": clock_wall or int(time.time() * 1000),
            "descriptors": [descriptor] if descriptor else [],
            "displayTimeoutMSec": 10000,
            "edgeType": edge,
            "eventId": event_id,
            "objectTypes": [obj.value],
            "zonesStatus": {},
        }
        await self.event_sender("EventSmartDetect", payload)

    # ------------------------------------------------------------------
    # Snapshot helpers
    # ------------------------------------------------------------------

    async def _fetch_event_snapshot(self, fid: str, unifi_id: int) -> None:
        http_url = self._cfg("http_url", "")
        if not http_url:
            return
        url = f"{http_url.rstrip('/')}/api/events/{fid}/snapshot.jpg"
        try:
            async with aiohttp.ClientSession() as session:
                async with session.get(url, timeout=aiohttp.ClientTimeout(total=5)) as r:
                    if r.status == 200:
                        data = await r.read()
                        f = tempfile.NamedTemporaryFile(delete=False, suffix=".jpg")
                        f.write(data)
                        f.close()
                        ev = self._active_events.get(unifi_id)
                        if ev:
                            ev["snapshot_path"] = f.name
        except Exception as exc:
            self.log.debug("Snapshot fetch for fid=%s failed: %s", fid, exc)

    # ------------------------------------------------------------------
    # Descriptor builder
    # ------------------------------------------------------------------

    def _build_descriptor(self, msg: dict, obj: ObjectType) -> Optional[dict]:
        after = msg.get("after") or {}
        box = after.get("box")
        detect_w = int(self._cfg("detect_width", 1280))
        detect_h = int(self._cfg("detect_height", 720))
        time_sync = int(self._cfg("time_sync_ms", 0))

        if box and len(box) == 4:
            sx, sy = 1000 / detect_w, 1000 / detect_h
            x = int(box[0] * sx)
            y = int(box[1] * sy)
            w = int((box[2] - box[0]) * sx)
            h = int((box[3] - box[1]) * sy)
            coord = [x, y, w, h]
        else:
            coord = [0, 0, 1000, 1000]

        score = after.get("top_score") or after.get("score") or 0.95
        tracker_id = after.get("id", 1)
        if isinstance(tracker_id, str):
            tracker_id = abs(hash(tracker_id)) % 1_000_000

        start_ms = int(after.get("start_time", 0) * 1000) - time_sync

        return {
            "attributes": None,
            "confidenceLevel": int(score * 100),
            "coord": coord,
            "coord3d": [-1, -1],
            "depth": None,
            "firstShownTimeMs": start_ms,
            "idleSinceTimeMs": int(after.get("motionless_count", 0) * 1000),
            "lines": [],
            "loiterZones": [],
            "name": "",
            "objectType": obj.value,
            "secondLensZones": [],
            "speed": float(after["average_estimated_speed"]) if after.get("average_estimated_speed") else None,
            "stationary": bool(after.get("stationary", False)),
            "tag": "",
            "trackerID": tracker_id,
            "zones": [0],
        }
