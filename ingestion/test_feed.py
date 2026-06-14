"""
Test rapide d'un flux GTFS-RT : affiche un résumé sans Kafka ni Cassandra.
Usage : python test_feed.py <URL>
"""
import sys
import json
import requests
from google.transit import gtfs_realtime_pb2


def fetch(url: str) -> gtfs_realtime_pb2.FeedMessage:
    resp = requests.get(url, timeout=15, headers={"Accept": "application/x-protobuf"})
    resp.raise_for_status()
    feed = gtfs_realtime_pb2.FeedMessage()
    feed.ParseFromString(resp.content)
    return feed


def summarise(feed: gtfs_realtime_pb2.FeedMessage) -> None:
    from datetime import datetime, timezone

    ts = datetime.fromtimestamp(feed.header.timestamp, tz=timezone.utc)
    print(f"\n=== GTFS-RT Feed ===")
    print(f"  Version   : {feed.header.gtfs_realtime_version}")
    print(f"  Timestamp : {ts.isoformat()}")
    print(f"  Entités   : {len(feed.entity)}")

    vehicles, trips, alerts = [], [], []
    for entity in feed.entity:
        if entity.HasField("vehicle"):
            vehicles.append(entity.vehicle)
        if entity.HasField("trip_update"):
            trips.append(entity.trip_update)
        if entity.HasField("alert"):
            alerts.append(entity.alert)

    print(f"\n  VehiclePositions : {len(vehicles)}")
    print(f"  TripUpdates      : {len(trips)}")
    print(f"  Alerts           : {len(alerts)}")

    if vehicles:
        print("\n--- Exemple VehiclePosition ---")
        v = vehicles[0]
        print(json.dumps({
            "vehicle_id": v.vehicle.id,
            "label":      v.vehicle.label,
            "trip_id":    v.trip.trip_id,
            "route_id":   v.trip.route_id,
            "latitude":   v.position.latitude,
            "longitude":  v.position.longitude,
            "speed_kmh":  round(v.position.speed * 3.6, 1),
            "status":     v.current_status,
        }, indent=2, ensure_ascii=False))

    if trips:
        print("\n--- Exemple TripUpdate ---")
        tu = trips[0]
        stops = []
        for stu in tu.stop_time_update[:3]:
            s = {"stop_id": stu.stop_id, "seq": stu.stop_sequence}
            if stu.HasField("arrival"):
                s["arrival_delay_s"] = stu.arrival.delay
            stops.append(s)
        print(json.dumps({
            "trip_id":  tu.trip.trip_id,
            "route_id": tu.trip.route_id,
            "delay_s":  tu.delay,
            "stops":    stops,
        }, indent=2, ensure_ascii=False))

    routes = {v.trip.route_id for v in vehicles if v.trip.route_id}
    if routes:
        print(f"\n  Lignes détectées ({len(routes)}) : {', '.join(sorted(routes)[:15])}")


if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python test_feed.py <GTFS_RT_URL>")
        sys.exit(1)
    feed = fetch(sys.argv[1])
    summarise(feed)
