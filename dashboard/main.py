"""
Backend FastAPI — expose les données GTFS via REST et WebSocket.
Interroge Trino, qui lit Cassandra via son connecteur officiel.
"""
from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.templating import Jinja2Templates
from fastapi.requests import Request
import trino
import asyncio
from datetime import date

app = FastAPI(title="GTFS-RT Dashboard")
templates = Jinja2Templates(directory="templates")


def get_trino_cursor():
    conn = trino.dbapi.connect(
        host="trino",
        port=8080,
        user="dashboard",
        catalog="cassandra",
        schema="gtfs",
    )
    return conn.cursor()


def query(sql: str) -> list[dict]:
    cur = get_trino_cursor()
    cur.execute(sql)
    cols = [d[0] for d in cur.description]
    return [dict(zip(cols, row)) for row in cur.fetchall()]


def today() -> str:
    return date.today().strftime("%Y%m%d")


# ── Vehicle Positions ─────────────────────────────────────────────────────────

def fetch_live_positions() -> list[dict]:
    """Derniere position GPS de chaque vehicule (60 dernieres secondes)."""
    return query("""
        WITH ranked AS (
            SELECT
                vehicle_id, route_id,
                latitude, longitude,
                ROUND(CAST(speed AS double) * 3.6, 1) AS vitesse_kmh,
                bearing, current_status,
                ROW_NUMBER() OVER (
                    PARTITION BY vehicle_id
                    ORDER BY ingestion_time DESC
                ) AS rn
            FROM vehicle_positions
            WHERE ingestion_time > (to_unixtime(now()) - 60)
        )
        SELECT vehicle_id, route_id, latitude, longitude,
               vitesse_kmh, bearing, current_status
        FROM ranked
        WHERE rn = 1
          AND latitude  BETWEEN 59.9 AND 60.5
          AND longitude BETWEEN 24.5 AND 25.5
    """)


def fetch_vehicle_stats() -> dict:
    """Stats vehicules (compte, vitesse moyenne)."""
    rows = query("""
        SELECT
            COUNT(DISTINCT vehicle_id) AS vehicules_actifs,
            ROUND(CAST(AVG(speed) AS double) * 3.6, 1) AS vitesse_moy_kmh
        FROM vehicle_positions
        WHERE ingestion_time > (to_unixtime(now()) - 60)
    """)
    return rows[0] if rows else {"vehicules_actifs": 0, "vitesse_moy_kmh": 0.0}


# ── Trip Updates ──────────────────────────────────────────────────────────────

def fetch_top_delays() -> list[dict]:
    """Top 10 lignes les plus en retard — scan complet, filtré côté Trino."""
    return query("""
        SELECT
            route_id,
            ROUND(CAST(AVG(avg_delay_seconds) AS double) / 60.0, 1) AS retard_min,
            COUNT(*) AS nb_trajets
        FROM trip_updates
        WHERE avg_delay_seconds > 0
        GROUP BY route_id
        HAVING COUNT(*) > 3
        ORDER BY retard_min DESC
        LIMIT 10
    """)


def fetch_global_stats_updates() -> dict:
    """Statistiques globales sur les trip updates."""
    rows = query("""
        SELECT
            COUNT(*)  AS total_trajets,
            COUNT(DISTINCT route_id) AS lignes_actives,
            ROUND(CAST(AVG(avg_delay_seconds) AS double) / 60.0, 2) AS retard_moyen_min
        FROM trip_updates
    """)
    return rows[0] if rows else {"total_trajets": 0, "lignes_actives": 0, "retard_moyen_min": 0.0}


# ── Service Alerts ────────────────────────────────────────────────────────────

def fetch_recent_alerts() -> list[dict]:
    """20 dernières alertes."""
    return query("""
        SELECT
            ingestion_time,
            header,
            description,
            start_time,
            end_time
        FROM service_alerts
        ORDER BY ingestion_time DESC
        LIMIT 20
    """)


def fetch_alert_count() -> dict:
    """Nombre total d'alertes."""
    rows = query("SELECT COUNT(*) AS total_alerts FROM service_alerts")
    return rows[0] if rows else {"total_alerts": 0}


# ── Routes HTTP ───────────────────────────────────────────────────────────────

@app.get("/")
async def index(request: Request):
    return templates.TemplateResponse("index.html", {"request": request})

@app.get("/api/delays")
def api_delays():
    return fetch_top_delays()

@app.get("/api/positions")
def api_positions():
    return fetch_live_positions()

@app.get("/api/stats")
def api_stats():
    vp = fetch_vehicle_stats()
    tu = fetch_global_stats_updates()
    alerts = fetch_alert_count()
    return {**vp, **tu, **alerts}

@app.get("/api/alerts")
def api_alerts():
    return fetch_recent_alerts()

@app.get("/api/trajectory/{vehicle_id}")
def api_trajectory(vehicle_id: str):
    """Trajet complet d'un vehicule aujourd'hui."""
    return query(f"""
        SELECT latitude, longitude, ingestion_time, speed,
               bearing, current_status
        FROM vehicle_positions
        WHERE vehicle_id = '{vehicle_id}'
          AND start_date = '{today()}'
        ORDER BY ingestion_time ASC
    """)


# ── WebSocket ─────────────────────────────────────────────────────────────────

@app.websocket("/ws/dashboard")
async def ws_dashboard(ws: WebSocket):
    await ws.accept()
    try:
        while True:
            await ws.send_json({
                "positions": fetch_live_positions(),
                "delays":    fetch_top_delays(),
                "stats":     {**fetch_vehicle_stats(), **fetch_global_stats_updates()},
                "alerts":    fetch_recent_alerts()[:5],
            })
            await asyncio.sleep(5)
    except WebSocketDisconnect:
        pass
