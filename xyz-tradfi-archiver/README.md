# xyz-tradfi-archiver

Standalone Hyperliquid XYZ tradfi perps archiver.

The project is intentionally not coupled to Mosaic. It records raw Hyperliquid REST and WebSocket data into `brut/`, then writes normalized Parquet datasets into `parquet/`.

## Outputs

```text
s3://xyz-tradfi-archive/
  brut/
  parquet/
  manifests/
  checksums/
  reports/
```

## Covered Hyperliquid sources

```text
REST /info:
  perpDexs
  metaAndAssetCtxs
  allMids
  l2Book
  candleSnapshot
  fundingHistory

WebSocket:
  l2Book
  trades
```

## Start

```bash
cp .env.example .env
docker compose up --build
```

## Commands

```bash
xyz-archiver record
xyz-archiver write
xyz-archiver validate-once
xyz-archiver validate-loop
xyz-archiver inspect
```

## Notes

`brut/` is append-only and compressed as JSONL Zstandard. `parquet/` is derived and can be rebuilt from `brut/` if the schema evolves.
