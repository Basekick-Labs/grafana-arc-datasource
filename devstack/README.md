# devstack — local Grafana against production Arc

Reproduces the watchdog stack locally so plugin changes can be verified
against **real data** before they are released. Every bug in the 1.3.3-1.3.8
run reached production because there was no such loop.

## Setup

```bash
cp devstack/.env.example devstack/.env   # then paste a read-scoped Arc token
npm run build && mage -v                 # build frontend + backend into dist/
docker compose -f devstack/docker-compose.yaml up -d
```

Grafana: http://localhost:3001 (anonymous admin; no login needed).
The **Arc (prod)** datasource is provisioned automatically.

## After changing the plugin

```bash
npm run build && mage -v
docker compose -f devstack/docker-compose.yaml restart grafana
```

## Notes

- Grafana is pinned to **12.4.2**, matching h01. The scaffold's own
  `docker-compose.yaml` uses grafana-enterprise 13.1.0, which can mask or
  invent version-specific behaviour.
- This reads production data. Use a **read-scoped** token; nothing here writes.
- `devstack/.env` is gitignored — the token must never be committed.
