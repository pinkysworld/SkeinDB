async function rpc(method, params = {}) {
  const baseUrl = document.getElementById('baseUrl').value.trim().replace(/\/$/, '');
  const token = document.getElementById('token').value.trim();

  const req = {
    skeinql: "1.0",
    id: "ui-" + Math.random().toString(16).slice(2),
    method,
    params,
  };

  const headers = { "Content-Type": "application/json" };
  if (token) headers["Authorization"] = "Bearer " + token;

  const res = await fetch(baseUrl + "/api/v1/rpc", {
    method: "POST",
    headers,
    body: JSON.stringify(req),
  });

  const text = await res.text();
  let parsed;
  try { parsed = JSON.parse(text); } catch { parsed = { raw: text }; }

  return { status: res.status, body: parsed };
}

function setOut(obj) {
  document.getElementById('out').textContent = JSON.stringify(obj, null, 2);
}

document.getElementById('btnVersion').addEventListener('click', async () => {
  try {
    setOut({ loading: true });
    const r = await rpc("system.version", {});
    setOut(r);
  } catch (e) {
    setOut({ error: String(e) });
  }
});

document.getElementById('btnStats').addEventListener('click', async () => {
  try {
    setOut({ loading: true });
    const r = await rpc("stats.snapshot", {});
    setOut(r);
  } catch (e) {
    setOut({ error: String(e) });
  }
});
