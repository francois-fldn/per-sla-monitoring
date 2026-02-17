import express from "express";
import fetch from "node-fetch";

const app = express();

const port = Number(process.env.PORT ?? "3000");
const orionBaseUrl = process.env.ORION_LD_BASE_URL ?? "http://orion-ld:1026";
const ownerAttr = process.env.OWNER_ATTR ?? "ownerId";
const timeAttr = process.env.TIME_ATTR ?? "observedAt";
const timeoutMs = Number(process.env.ORION_TIMEOUT_MS ?? "10000");

function withTimeout(ms) {
  const controller = new AbortController();
  const t = setTimeout(() => controller.abort(), ms);
  return { controller, done: () => clearTimeout(t) };
}

function buildQ({ ownerId, from, to }) {
  const parts = [];

  // owner filter => q=ownerId==abc
  if (ownerId) parts.push(`${ownerAttr}==${JSON.stringify(String(ownerId))}`);

  // time range (best-effort): requires your entities to have a comparable attribute (date/time as ISO string)
  // Example: q=observedAt>= "2026-01-01T00:00:00Z";observedAt<= "2026-01-02T00:00:00Z"
  if (from) parts.push(`${timeAttr}>=${JSON.stringify(String(from))}`);
  if (to) parts.push(`${timeAttr}<=${JSON.stringify(String(to))}`);

  return parts.length ? parts.join(";") : undefined;
}

async function orionGet(path, query) {
  const url = new URL(`${orionBaseUrl}${path}`);
  for (const [k, v] of Object.entries(query ?? {})) {
    if (v !== undefined && v !== null && v !== "") url.searchParams.set(k, String(v));
  }

  const { controller, done } = withTimeout(timeoutMs);
  const res = await fetch(url, {
    method: "GET",
    headers: { Accept: "application/ld+json" },
    signal: controller.signal
  }).finally(done);

  if (!res.ok) {
    const text = await res.text().catch(() => "");
    throw new Error(`Orion-LD GET failed: ${res.status} ${res.statusText} ${text}`);
  }
  return res.json();
}

// List / query entities
app.get("/entities", async (req, res) => {
  try {
    const {
      type,
      id,
      ownerId,
      from,
      to,
      limit = "100",
      offset = "0",
      attrs
    } = req.query;

    const q = buildQ({ ownerId, from, to });

    const data = await orionGet("/ngsi-ld/v1/entities", {
      type,
      id,
      q,
      limit,
      offset,
      attrs
    });

    res.json(data);
  } catch (e) {
    res.status(502).json({ error: String(e?.message ?? e) });
  }
});

// Get entity by id (full)
app.get("/entities/:id", async (req, res) => {
  try {
    const data = await orionGet(`/ngsi-ld/v1/entities/${encodeURIComponent(req.params.id)}`, {});
    res.json(data);
  } catch (e) {
    res.status(502).json({ error: String(e?.message ?? e) });
  }
});

// Basic health: checks Orion version
app.get("/health", async (_req, res) => {
  try {
    const data = await orionGet("/version", {});
    res.json({ ok: true, orion: data });
  } catch (e) {
    res.status(502).json({ ok: false, error: String(e?.message ?? e) });
  }
});

app.listen(port, "0.0.0.0");
