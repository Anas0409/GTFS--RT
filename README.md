# Big Data — Transports Publics Temps Réel (GTFS-RT)

Pipeline de données temps réel pour le suivi des bus et trams via le protocole GTFS-RT.

## Architecture

```
API GTFS-RT (externe)
       │
       ▼
  Python Producer  ──►  Kafka (topics: gtfs_vehicle_positions / gtfs_trip_updates)
                                │
                                ▼
                        Python Consumer
                                │
                                ▼
                          Cassandra (keyspace: gtfs)
                                │
                                ▼
                            Trino (SQL analytique)
                                │
                                ▼
                          Metabase (dashboards)
```

## Stack technique

| Service    | Image                         | Rôle                              | Port  |
|------------|-------------------------------|-----------------------------------|-------|
| Zookeeper  | confluentinc/cp-zookeeper:7.4 | Coordination Kafka                | 2181  |
| Kafka      | confluentinc/cp-kafka:7.4     | Bus de messages temps réel        | 9092  |
| Cassandra  | cassandra:4.1                 | Stockage scalable des positions   | 9042  |
| Trino      | trinodb/trino:426             | Requêtage SQL analytique          | 8080  |
| Metabase   | metabase/metabase:latest      | Dashboards et visualisation       | 3000  |
| Ingestion  | (build local)                 | Producer + Consumer Python        | —     |

## Structure du projet

```
├── ingestion/
│   ├── producer.py       # Fetch GTFS-RT → publie sur Kafka
│   ├── consumer.py       # Consomme Kafka → écrit dans Cassandra
│   ├── main.py           # Point d'entrée (lance les deux)
│   ├── test_feed.py      # Test rapide d'une URL GTFS-RT
│   ├── requirements.txt
│   └── Dockerfile
├── cassandra/
│   └── init.cql          # Keyspace + schéma des tables
├── trino/
│   ├── catalog/
│   │   └── cassandra.properties   # Connecteur Trino → Cassandra
│   └── queries.sql       # Requêtes analytiques prêtes à l'emploi
├── docker-compose.yml
├── .env                  # Variables d'environnement (à créer)
├── .env.example          # Modèle
└── README.md
```

## Prérequis

- [Docker Desktop](https://www.docker.com/products/docker-desktop/) (v24+)
- Une URL de flux GTFS-RT publique (voir exemples ci-dessous)

## Lancement

### 1. Configurer l'environnement

```bash
cp .env.example .env
```

Éditer `.env` et renseigner l'URL du flux GTFS-RT :

```env
GTFS_RT_URL=https://votre-url-gtfs-rt
FETCH_INTERVAL=30
```

> ⚠️ Certains opérateurs exposent les positions véhicules et les retards
> sur **deux endpoints distincts** (ex: SNCF). Dans ce cas, renseigner en plus
> `GTFS_RT_TRIPUPDATES_URL` dans `.env` pour que la table `trip_updates` soit
> alimentée (sinon les requêtes de retard de `trino/queries.sql` resteront vides).

Exemples de flux publics gratuits :

| Réseau | GTFS_RT_URL (positions) | GTFS_RT_TRIPUPDATES_URL (retards) |
|--------|--------------------------|-------------------------------------|
| SNCF TER | `https://proxy.transport.data.gouv.fr/resource/sncf-ter-gtfs-rt-vehicle-positions` | `https://proxy.transport.data.gouv.fr/resource/sncf-ter-gtfs-rt-trip-updates` |
| IDFM | `https://prim.iledefrance-mobilites.fr/marketplace/gtfs-rt` | — (déjà inclus dans le flux principal) |
| Bordeaux TBM | `https://gtfs.infotbm.com/realtime/vehiclePositions` | — |

### 2. (Optionnel) Tester l'URL sans Docker

```bash
pip install gtfs-realtime-bindings requests
python ingestion/test_feed.py https://votre-url-gtfs-rt
```

Affiche un résumé du flux : nombre de véhicules, lignes détectées, exemple de position.

### 3. Démarrer la stack

```bash
docker-compose up -d
```

Ordre de démarrage automatique géré par les health checks :

```
Zookeeper → Kafka → Cassandra → cassandra-init → Trino → Metabase
                                              └──────────────────► Ingestion
```

### 4. Vérifier que tout tourne

```bash
docker-compose ps
```

Tous les services doivent être `healthy` ou `running`.  
L'init Cassandra (`cassandra-init`) se termine puis s'arrête — c'est normal.

### 5. Vérifier les logs d'ingestion

```bash
docker-compose logs -f ingestion
```

Sortie attendue :

```
producer  INFO  Published — vehicles: 142  trip_updates: 0
consumer  INFO  Consumer ready — topics: gtfs_vehicle_positions, gtfs_trip_updates
```

## Accès aux services

| Service   | URL                   | Identifiants        |
|-----------|-----------------------|---------------------|
| Trino UI  | http://localhost:8080 | user: `admin` (pas de mdp) |
| Metabase  | http://localhost:3000 | Créer un compte au 1er accès |
| Cassandra | localhost:9042        | cqlsh (pas d'auth)  |
| Kafka     | localhost:9092        | —                   |

## Connecter Metabase à Trino

1. Ouvrir http://localhost:3000 → **Paramètres → Bases de données → Ajouter**
2. Choisir **Presto**
3. Remplir :

   | Champ    | Valeur      |
   |----------|-------------|
   | Host     | `localhost` |
   | Port     | `8080`      |
   | Catalog  | `cassandra` |
   | Schema   | `gtfs`      |
   | Username | `admin`     |

4. Cliquer **Enregistrer** puis utiliser les requêtes de `trino/queries.sql`.

## Requêtes Trino utiles

Se connecter au CLI Trino :

```bash
docker exec -it trino trino
```

Exemples :

```sql
-- Véhicules actifs maintenant
SELECT vehicle_id, route_id, latitude, longitude, timestamp
FROM cassandra.gtfs.vehicle_positions
WHERE bucket = CAST(current_date AS VARCHAR)
ORDER BY timestamp DESC LIMIT 20;

-- Retard moyen par ligne
SELECT route_id, ROUND(AVG(delay)/60.0, 1) AS retard_min
FROM cassandra.gtfs.trip_updates
WHERE timestamp >= NOW() - INTERVAL '1' HOUR
GROUP BY route_id ORDER BY retard_min DESC;
```

Toutes les requêtes analytiques sont dans `trino/queries.sql`.

## Inspecter Cassandra

```bash
docker exec -it cassandra cqlsh

USE gtfs;
SELECT COUNT(*) FROM vehicle_positions;
SELECT vehicle_id, route_id, latitude, longitude, timestamp
FROM vehicle_positions LIMIT 10;
```

## Arrêter le projet

```bash
# Arrêter sans supprimer les données
docker-compose stop

# Arrêter et supprimer les conteneurs (données conservées dans les volumes)
docker-compose down

# Tout supprimer, y compris les données
docker-compose down -v
```

## GitHub — Pousser le code

### Première fois (dépôt déjà configuré)

```bash
git add .
git commit -m "feat: initial pipeline GTFS-RT"
git push origin main
```

### Workflow quotidien

```bash
# Voir les fichiers modifiés
git status

# Ajouter tous les changements
git add .

# Ou ajouter un fichier spécifique
git add ingestion/producer.py

# Créer un commit
git commit -m "feat: description du changement"

# Pousser vers GitHub
git push origin main
```

### Travailler sur une branche (recommandé)

```bash
# Créer et basculer sur une nouvelle branche
git checkout -b feature/nom-de-la-feature

# ... faire les modifications ...

git add .
git commit -m "feat: description"
git push origin feature/nom-de-la-feature

# Fusionner dans main via Pull Request sur GitHub
```

### Commandes utiles

```bash
# Historique des commits
git log --oneline

# Annuler les modifications non commitées d'un fichier
git checkout -- nom-du-fichier

# Voir les différences avant de commiter
git diff

# Récupérer les derniers changements depuis GitHub
git pull origin main
```

### Dépôt GitHub

https://github.com/Anas0409/Big-Data-Transports-publics-temps-r-el

## Auteur

Anas Benhaddane — Projet Big Data, Université Ibn Tofail
