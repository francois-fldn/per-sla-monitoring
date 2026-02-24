# Grafana provisioning (per-sla-monitoring)

- Grafana: http://localhost:3001 (admin/admin by default)
- Datasource `ngsi-api` uses the JSON API connector and calls `http://ngsi-api:3000` inside the compose network.

Adjust the dashboard query:
- `type` query param (e.g. `Device`)
- add `ownerId`, `from`, `to`, `attrs` per your `ngsi-api` implementation.
