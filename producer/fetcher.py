"""
Télécharge et parse les flux GTFS-RT HSL.
Retourne des dicts Python prêts à sérialiser en JSON.
"""
import time
import logging
import requests
from google.transit import gtfs_realtime_pb2
import config

logger = logging.getLogger(__name__)


def _download(url: str) -> bytes | None:
    try:
        r = requests.get(
            url,
            headers={"User-Agent": config.USER_AGENT},
            timeout=30
        )
        r.raise_for_status()
        return r.content
    except Exception as e:
        logger.error(f"Download failed [{url}]: {e}")
        return None


def fetch_vehicle_positions() -> list[dict]:
    raw = _download(config.GTFS_VEHICLE_URL)
    if not raw:
        return []

    feed = gtfs_realtime_pb2.FeedMessage()
    feed.ParseFromString(raw)

    now = int(time.time())
    results = []

    STATUS_MAP = {
        0: "INCOMING_AT",
        1: "STOPPED_AT",
        2: "IN_TRANSIT_TO",
    }

    if not feed.entity:
        logger.info("  vehicle_positions : feed empty (no active vehicles)")
        return []

    for entity in feed.entity:
        v = entity.vehicle
        if not v.vehicle.id:
            continue

        full_id = v.vehicle.id
        parts = full_id.split("/", 1)
        operator_id = parts[0] if len(parts) > 1 else ""
        vehicle_id  = parts[1] if len(parts) > 1 else parts[0]

        results.append({
            "operator_id":     operator_id,
            "vehicle_id":      vehicle_id,
            "full_vehicle_id": full_id,
            "route_id":        v.trip.route_id if v.HasField("trip") else "",
            "direction_id":    v.trip.direction_id if v.HasField("trip") else 0,
            "start_date":      v.trip.start_date   if v.HasField("trip") else "",
            "start_time":      v.trip.start_time   if v.HasField("trip") else "",
            "latitude":        v.position.latitude,
            "longitude":       v.position.longitude,
            "bearing":         v.position.bearing,
            "speed":           v.position.speed,
            "current_status":  STATUS_MAP.get(v.current_status, "UNKNOWN"),
            "gtfs_timestamp":  v.timestamp,
            "ingestion_time":  now,
        })

    return results


def fetch_trip_updates() -> list[dict]:
    raw = _download(config.GTFS_TRIP_URL)
    if not raw:
        return []

    feed = gtfs_realtime_pb2.FeedMessage()
    feed.ParseFromString(raw)

    now = int(time.time())
    results = []

    for entity in feed.entity:
        if not entity.HasField("trip_update"):
            continue
        tu   = entity.trip_update
        trip = tu.trip

        delays = []
        uncertainties = []
        for stu in tu.stop_time_update:
            if stu.HasField("arrival"):
                delays.append(stu.arrival.delay)
                if stu.arrival.uncertainty:
                    uncertainties.append(stu.arrival.uncertainty)
            if stu.HasField("departure"):
                delays.append(stu.departure.delay)
                if stu.departure.uncertainty:
                    uncertainties.append(stu.departure.uncertainty)

        results.append({
            "route_id":            trip.route_id      if trip.route_id    else "",
            "start_date":          trip.start_date    if trip.start_date  else "",
            "start_time":          trip.start_time    if trip.start_time  else "",
            "direction_id":        trip.direction_id,
            "avg_delay_seconds":   sum(delays) / len(delays) if delays else 0.0,
            "max_uncertainty":     max(uncertainties) if uncertainties else 0,
            "num_stops":           len(tu.stop_time_update),
            "gtfs_timestamp":      now,
            "ingestion_time":      now,
        })

    return results


def fetch_service_alerts() -> list[dict]:
    raw = _download(config.GTFS_ALERTS_URL)
    if not raw:
        return []

    feed = gtfs_realtime_pb2.FeedMessage()
    feed.ParseFromString(raw)

    now = int(time.time())
    results = []

    for entity in feed.entity:
        if not entity.HasField("alert"):
            continue

        a = entity.alert

        results.append({
            "header": a.header_text.translation[0].text if a.header_text.translation else "",
            "description": a.description_text.translation[0].text if a.description_text.translation else "",
            "start_time": a.active_period[0].start if a.active_period else None,
            "end_time": a.active_period[0].end if a.active_period else None,
            "ingestion_time": now
        })

    return results
