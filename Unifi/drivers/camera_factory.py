from __future__ import annotations
from typing import Dict, Any
from Unifi.drivers.camera_driver import CameraDriver
from Unifi.drivers.amcrest import AmcrestDriver
from Unifi.drivers.frigate import FrigateDriver
from Unifi.drivers.null import NullDriver

def build_camera_driver(settings: Dict[str, Any], log) -> CameraDriver:
    brand = (settings.get("camera.type") or settings.get("camera_type") or "null").lower()
    if brand == "amcrest":
        amc = settings.get("amcrest", settings)
        return AmcrestDriver(amc, log)
    elif brand == "frigate":
        return FrigateDriver(settings, log)
    # elif brand == "hikvision": return HikvisionDriver(...)
    # elif brand == "reolink": return ReolinkDriver(...)
    return NullDriver(settings, log)