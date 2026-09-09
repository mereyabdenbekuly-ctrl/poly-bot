from __future__ import annotations

import json
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Any
from urllib.parse import urlparse

from polybot.storage import Storage

_HTML = """<!doctype html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>Polybot — autonomous observer</title>
<style>
body { font: 15px system-ui, sans-serif; max-width: 1100px; margin: 32px auto;
       padding: 0 18px; background: #f6f7f9; color: #17202a }
pre { white-space: pre-wrap; overflow: auto; background: #fff;
      border: 1px solid #ddd; border-radius: 8px; padding: 14px }
small { color: #68737d }
</style>
</head>
<body>
<h1>Polybot</h1>
<p><span id="state">loading…</span> · view-only dashboard</p>
<p><small>Refreshes automatically. This page never starts a scan and never places
orders.</small></p>
<pre id="data">loading…</pre>
<script>
async function refresh() {
  try {
    const response = await fetch('/api/dashboard', {cache: 'no-store'});
    const data = await response.json();
    document.getElementById('state').textContent = 'updated ' + data.generated_at;
    document.getElementById('data').textContent = JSON.stringify(data, null, 2);
  } catch (error) {
    document.getElementById('data').textContent = String(error);
  }
}
refresh();
setInterval(refresh, 30000);
</script>
</body>
</html>"""


def serve_dashboard(storage: Storage, *, host: str, port: int) -> None:
    class Handler(BaseHTTPRequestHandler):
        def do_GET(self) -> None:  # noqa: N802
            path = urlparse(self.path).path
            if path == "/health":
                self._send_json({"ok": True})
            elif path in {"/api/dashboard", "/api/status"}:
                self._send_json(storage.dashboard_payload())
            elif path == "/":
                body = _HTML.encode("utf-8")
                self.send_response(200)
                self.send_header("Content-Type", "text/html; charset=utf-8")
                self.send_header("Cache-Control", "no-store")
                self.send_header("Content-Length", str(len(body)))
                self.end_headers()
                self.wfile.write(body)
            else:
                self.send_error(404)

        def _send_json(self, payload: dict[str, Any]) -> None:
            body = json.dumps(payload, ensure_ascii=False, default=str).encode("utf-8")
            self.send_response(200)
            self.send_header("Content-Type", "application/json; charset=utf-8")
            self.send_header("Cache-Control", "no-store")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        def log_message(self, format: str, *args: object) -> None:
            return

    ThreadingHTTPServer((host, port), Handler).serve_forever()
