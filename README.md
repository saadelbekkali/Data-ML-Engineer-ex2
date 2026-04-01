# LastFM Session Analysis

Spark job written in Scala that finds the **top 10 most-played songs** within the **top 50 longest listening sessions** from the Last.fm 1K dataset.

## What it does

1. Reads raw Last.fm play events (TSV format)
2. Groups consecutive plays per user into **sessions** — a new session starts when the gap between two consecutive tracks exceeds **20 minutes**
3. Ranks sessions by track count and keeps the **top 50**
4. Within those sessions, counts plays per song and returns the **top 10**

## Project structure

```
├── src/main/scala/
│   ├── Main.scala              # Entry point — session logic & top 10 computation
│   ├── schema/LastFmSchema.scala  # TSV schema definition
│   └── utils/SparkUtils.scala     # SparkSession builder
├── build.sbt                   # sbt build config (Scala 2.12, Spark 3.5.1)
├── project/
│   ├── build.properties        # sbt version
│   └── plugins.sbt             # sbt-assembly plugin
└── docker-compose.yml          # Runs spark-submit via Bitnami Spark image
```

## Requirements

- Java 11+
- sbt
- Docker + Docker Compose

## Setup

1. Download the [Last.fm 1K dataset](http://ocelma.net/MusicRecommendationDataset/lastfm-1K.html)
2. Place the `.tsv` file inside `data/`

## How to run

```bash
# Build the fat jar
sbt assembly

# Run with Docker
docker-compose up
```

Output is saved to `output/output-top10-songs/` as a CSV and printed to the console.

## Configuration

| Environment variable | Default                                              | Description              |
|----------------------|------------------------------------------------------|--------------------------|
| `INPUT_PATH`         | `/data/userid-timestamp-artid-artname-traid-traname.tsv` | Path to the input TSV |
| `OUTPUT_PATH`        | `/output/output-top10-songs`                         | Path for CSV output      |

These can be overridden in `docker-compose.yml`.

## Input format

Tab-separated file with no header, columns in order:

| Column          | Type   |
|-----------------|--------|
| `user_id`       | String |
| `timestamp`     | String (`yyyy-MM-dd'T'HH:mm:ss'Z'`) |
| `artist_mb_id`  | String |
| `artist_name`   | String |
| `track_mb_id`   | String |
| `track_name`    | String |