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

// --- UI helpers ---
function pickQuery(req) {
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

  return {
    type,
    id,
    ownerId,
    from,
    to,
    limit,
    offset,
    attrs,
    q
  };
}

function queryToOrionParams(qry) {
  return {
    type: qry.type,
    id: qry.id,
    q: qry.q,
    limit: qry.limit,
    offset: qry.offset,
    attrs: qry.attrs
  };
}

// --- Simple UI (no build step, single file) ---
app.get("/ui", (_req, res) => {
  res.type("html").send(`<!doctype html>
<html>
<head>
  <meta charset="utf-8"/>
  <meta name="viewport" content="width=device-width, initial-scale=1"/>
  <title>FIWARE NGSI-LD Query UI</title>
  <style>
    body { font-family: system-ui, sans-serif; margin: 16px; }
    .row { display: grid; grid-template-columns: 160px 1fr; gap: 8px; margin-bottom: 8px; max-width: 880px; }
    input, textarea { width: 100%; padding: 6px; }
    button { padding: 8px 12px; }
    pre { background: #0b1020; color: #e6edf3; padding: 12px; overflow: auto; max-width: 1000px; }
    .hint { color: #555; font-size: 12px; margin: 6px 0 12px; }
  </style>
</head>
<body>
  <h2>NGSI-LD (Orion-LD) query</h2>
  <div class="hint">
    This calls <code>/entities</code> on this API, which proxies Orion-LD. Dates are applied via the configured <code>TIME_ATTR</code>.
  </div>

  <div class="row"><label>type</label><input id="type" placeholder="Device"/></div>
  <div class="row"><label>id</label><input id="id" placeholder="urn:ngsi-ld:Device:001"/></div>
  <div class="row"><label>ownerId</label><input id="ownerId" placeholder="tenant/user id"/></div>
  <div class="row"><label>from (ISO)</label><input id="from" placeholder="2026-01-01T00:00:00Z"/></div>
  <div class="row"><label>to (ISO)</label><input id="to" placeholder="2026-01-02T00:00:00Z"/></div>
  <div class="row"><label>attrs</label><input id="attrs" placeholder="temperature,humidity"/></div>
  <div class="row"><label>limit</label><input id="limit" value="100"/></div>
  <div class="row"><label>offset</label><input id="offset" value="0"/></div>

  <button id="run">Run query</button>
  <button id="copy">Copy API URL</button>

  <h3>Result</h3>
  <pre id="out">(no data)</pre>

<script>
  function buildUrl() {
    const p = new URLSearchParams();
    for (const [k, el] of Object.entries({
      type: type, id: id, ownerId: ownerId, from: from, to: to, attrs: attrs, limit: limit, offset: offset
    })) {
      const v = el.value.trim();
      if (v) p.set(k, v);
    }
    return '/entities?' + p.toString();
  }

  run.onclick = async () => {
    out.textContent = 'Loading...';
    const url = buildUrl();
    try {
      const r = await fetch(url, { headers: { 'Accept': 'application/json' }});
      const t = await r.text();
      out.textContent = t ? JSON.stringify(JSON.parse(t), null, 2) : '(empty)';
    } catch (e) {
      out.textContent = String(e);
    }
  };

  copy.onclick = async () => {
    const url = location.origin + buildUrl();
    try { await navigator.clipboard.writeText(url); } catch {}
    alert(url);
  };
</script>
</body>
</html>`);
});

// Optional JSON endpoint for UI if you prefer a stable route
app.get("/ui/query", async (req, res) => {
  try {
    const qry = pickQuery(req);
    const data = await orionGet("/ngsi-ld/v1/entities", queryToOrionParams(qry));
    res.json({ query: qry, data });
  } catch (e) {
    res.status(502).json({ error: String(e?.message ?? e) });
  }
});

// List / query entities
app.get("/entities", async (req, res) => {
  try {
    const qry = pickQuery(req);

    const data = await orionGet("/ngsi-ld/v1/entities", queryToOrionParams(qry));
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
