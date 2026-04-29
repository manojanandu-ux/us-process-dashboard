/**
 * US Process D1 Worker
 * Serves CSV records and HTML change data from Cloudflare D1.
 *
 * Routes:
 *   GET /api/uploads          — list recent uploads
 *   GET /api/records          — paginated CSV records (upload_id, state, page, limit)
 *   GET /api/html-changes     — paginated HTML changes (upload_id, group_key, page, limit)
 *   GET /api/stats            — aggregate counts for the latest upload
 */

const CORS = {
  "Access-Control-Allow-Origin":  "*",
  "Access-Control-Allow-Methods": "GET, POST, DELETE, OPTIONS",
  "Access-Control-Allow-Headers": "Content-Type",
  "Content-Type":                 "application/json",
};

function json(data, status = 200) {
  return new Response(JSON.stringify(data), { status, headers: CORS });
}

function err(msg, status = 400) {
  return json({ error: msg }, status);
}

function qs(url) {
  return Object.fromEntries(new URL(url).searchParams);
}

export default {
  async fetch(request, env) {
    if (request.method === "OPTIONS") {
      return new Response(null, { status: 204, headers: CORS });
    }

    const { pathname } = new URL(request.url);
    const p = qs(request.url);

    // Non-API routes → serve static assets (index.html, HTML change files, etc.)
    if (!pathname.startsWith("/api/")) {
      return env.ASSETS.fetch(request);
    }

    // ── /api/uploads ──────────────────────────────────────────────────────────
    if (pathname === "/api/uploads") {
      const limit = Math.min(parseInt(p.limit) || 50, 100);
      const { results } = await env.DB.prepare(
        "SELECT id, created_at, csv_file, html_file, csv_rows, html_items FROM uploads ORDER BY created_at DESC LIMIT ?"
      ).bind(limit).all();
      return json({ uploads: results });
    }

    // ── DELETE /api/uploads/:id ───────────────────────────────────────────────
    if (request.method === "DELETE" && pathname.match(/^\/api\/uploads\/[^/]+$/)) {
      const uploadId = pathname.split("/").pop();
      // Fetch html_file key before deleting so we can clean R2 too
      const uploadRow = await env.DB.prepare("SELECT html_file FROM uploads WHERE id = ?").bind(uploadId).first();
      await Promise.all([
        env.DB.prepare("DELETE FROM csv_records WHERE upload_id = ?").bind(uploadId).run(),
        env.DB.prepare("DELETE FROM html_changes WHERE upload_id = ?").bind(uploadId).run(),
      ]);
      await env.DB.prepare("DELETE FROM uploads WHERE id = ?").bind(uploadId).run();
      if (uploadRow?.html_file) {
        await env.HTML_BUCKET.delete(uploadRow.html_file).catch(() => {});
      }
      return json({ ok: true, deleted_id: uploadId });
    }

    // ── /api/records ──────────────────────────────────────────────────────────
    if (pathname === "/api/records") {
      const page  = Math.max(1, parseInt(p.page)  || 1);
      const limit = Math.min(parseInt(p.limit) || 50, 1000);
      const offset = (page - 1) * limit;

      // resolve upload_id: explicit or latest
      let uploadId = p.upload_id;
      if (!uploadId) {
        const row = await env.DB.prepare(
          "SELECT id FROM uploads ORDER BY created_at DESC LIMIT 1"
        ).first();
        if (!row) return json({ records: [], total: 0, page, limit });
        uploadId = row.id;
      }

      let whereClauses = ["upload_id = ?"];
      let binds = [uploadId];

      if (p.state)        { whereClauses.push("state = ?");         binds.push(p.state); }
      if (p.is_project)   { whereClauses.push("is_project = ?");    binds.push(p.is_project); }
      if (p.trade)        { whereClauses.push("trade = ?");          binds.push(p.trade); }
      if (p.project_type) { whereClauses.push("project_type = ?");   binds.push(p.project_type); }
      if (p.search) {
        whereClauses.push("(project_name LIKE ? OR agency LIKE ? OR summary LIKE ?)");
        const q = `%${p.search}%`;
        binds.push(q, q, q);
      }

      const where = whereClauses.join(" AND ");

      const { results: records } = await env.DB.prepare(
        `SELECT * FROM csv_records WHERE ${where} ORDER BY id LIMIT ? OFFSET ?`
      ).bind(...binds, limit, offset).all();

      const countRow = await env.DB.prepare(
        `SELECT COUNT(*) as n FROM csv_records WHERE ${where}`
      ).bind(...binds).first();

      return json({
        upload_id: uploadId,
        records,
        total: countRow?.n ?? 0,
        page,
        limit,
        pages: Math.ceil((countRow?.n ?? 0) / limit),
      });
    }

    // ── /api/html-changes ─────────────────────────────────────────────────────
    if (pathname === "/api/html-changes") {
      const page  = Math.max(1, parseInt(p.page)  || 1);
      const limit = Math.min(parseInt(p.limit) || 50, 1000);
      const offset = (page - 1) * limit;

      let uploadId = p.upload_id;
      if (!uploadId) {
        const row = await env.DB.prepare(
          "SELECT id FROM uploads ORDER BY created_at DESC LIMIT 1"
        ).first();
        if (!row) return json({ changes: [], total: 0, page, limit });
        uploadId = row.id;
      }

      let whereClauses = ["upload_id = ?"];
      let binds = [uploadId];

      if (p.group_key) { whereClauses.push("group_key = ?"); binds.push(p.group_key); }
      if (p.search) {
        whereClauses.push("(title LIKE ? OR change_summary LIKE ?)");
        const q = `%${p.search}%`;
        binds.push(q, q);
      }

      const where = whereClauses.join(" AND ");

      const { results: changes } = await env.DB.prepare(
        `SELECT id, upload_id, title, date, url, change_summary, changes_full_html, path, group_key
         FROM html_changes WHERE ${where} ORDER BY id LIMIT ? OFFSET ?`
      ).bind(...binds, limit, offset).all();

      const countRow = await env.DB.prepare(
        `SELECT COUNT(*) as n FROM html_changes WHERE ${where}`
      ).bind(...binds).first();

      // also return distinct group_keys for filter dropdowns
      const { results: groups } = await env.DB.prepare(
        "SELECT DISTINCT group_key FROM html_changes WHERE upload_id = ? ORDER BY group_key"
      ).bind(uploadId).all();

      return json({
        upload_id: uploadId,
        changes,
        total: countRow?.n ?? 0,
        page,
        limit,
        pages: Math.ceil((countRow?.n ?? 0) / limit),
        group_keys: groups.map(g => g.group_key),
      });
    }

    // ── /api/stats ────────────────────────────────────────────────────────────
    if (pathname === "/api/stats") {
      let uploadId = p.upload_id;
      if (!uploadId) {
        const row = await env.DB.prepare(
          "SELECT id FROM uploads ORDER BY created_at DESC LIMIT 1"
        ).first();
        if (!row) return json({ error: "No uploads found" }, 404);
        uploadId = row.id;
      }

      const [upload, csvCount, htmlCount, stateRows, tradeRows, isProjectRows] = await Promise.all([
        env.DB.prepare("SELECT * FROM uploads WHERE id = ?").bind(uploadId).first(),
        env.DB.prepare("SELECT COUNT(*) as n FROM csv_records WHERE upload_id = ?").bind(uploadId).first(),
        env.DB.prepare("SELECT COUNT(*) as n FROM html_changes WHERE upload_id = ?").bind(uploadId).first(),
        env.DB.prepare(
          "SELECT state, COUNT(*) as n FROM csv_records WHERE upload_id = ? AND state != '' GROUP BY state ORDER BY n DESC LIMIT 20"
        ).bind(uploadId).all(),
        env.DB.prepare(
          "SELECT trade, COUNT(*) as n FROM csv_records WHERE upload_id = ? AND trade != '' GROUP BY trade ORDER BY n DESC LIMIT 20"
        ).bind(uploadId).all(),
        env.DB.prepare(
          "SELECT is_project, COUNT(*) as n FROM csv_records WHERE upload_id = ? GROUP BY is_project"
        ).bind(uploadId).all(),
      ]);

      return json({
        upload_id: uploadId,
        upload,
        csv_records: csvCount?.n ?? 0,
        html_changes: htmlCount?.n ?? 0,
        by_state: stateRows.results,
        by_trade: tradeRows.results,
        by_is_project: isProjectRows.results,
      });
    }

    // ── /api/html-upload (POST) ────────────────────────────────────────────────
    if (pathname === "/api/html-upload" && request.method === "POST") {
      const form     = await request.formData();
      const file     = form.get("file");
      const uploadId = (form.get("upload_id") || "").trim();
      if (!file) return err("No file provided");
      const now      = new Date();
      const dateStr  = now.toISOString().slice(0, 10);
      const filename = file.name || "report.html";
      const key      = `${dateStr}/${now.getTime()}-${filename}`;
      await env.HTML_BUCKET.put(key, file.stream(), {
        httpMetadata:   { contentType: "text/html; charset=utf-8" },
        customMetadata: { originalName: filename, uploadedAt: now.toISOString(), size: String(file.size), uploadId },
      });
      // Link R2 key to D1 upload record so Manage Data can show it
      if (uploadId) {
        await env.DB.prepare("UPDATE uploads SET html_file = ? WHERE id = ?")
          .bind(key, uploadId).run();
      }
      return json({ ok: true, key, name: filename, date: dateStr, size: file.size });
    }

    // ── /api/html-files (GET list) ────────────────────────────────────────────
    if (pathname === "/api/html-files" && request.method === "GET") {
      const listed = await env.HTML_BUCKET.list({ limit: 500 });
      const files = listed.objects.map(o => ({
        key:      o.key,
        size:     o.customMetadata?.size ? parseInt(o.customMetadata.size) : o.size,
        uploaded: o.uploaded,
        name:     o.customMetadata?.originalName || o.key.split("/").pop(),
        date:     o.key.split("/")[0] || "",
      }));
      files.sort((a, b) => new Date(b.uploaded) - new Date(a.uploaded));
      return json({ files });
    }

    // ── /api/html-files/:key (GET raw HTML, DELETE) ───────────────────────────
    if (pathname.startsWith("/api/html-files/")) {
      const key = decodeURIComponent(pathname.slice("/api/html-files/".length));
      if (request.method === "GET") {
        const obj = await env.HTML_BUCKET.get(key);
        if (!obj) return err("Not found", 404);
        return new Response(obj.body, {
          headers: {
            "Content-Type":                "text/html; charset=utf-8",
            "Access-Control-Allow-Origin": "*",
            "Cache-Control":               "no-cache",
          },
        });
      }
      if (request.method === "DELETE") {
        await env.HTML_BUCKET.delete(key);
        // Also clear html_file on the linked upload record if upload_id provided
        const uid = p.upload_id || "";
        if (uid) {
          await env.DB.prepare("UPDATE uploads SET html_file = '' WHERE id = ?").bind(uid).run();
        }
        return json({ ok: true, deleted_key: key });
      }
      return err("Method not allowed", 405);
    }

    return err("Not found", 404);
  },
};
