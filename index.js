/**
 * US Process D1 Worker
 * Serves CSV records and HTML change data from Cloudflare D1.
 *
 * Routes:
 *   GET /api/uploads          — list recent uploads (with processed_date)
 *   GET /api/dates            — distinct processed dates with extraction metrics
 *   GET /api/records          — paginated CSV records (processed_date|upload_id, state, page, limit)
 *   GET /api/html-changes     — paginated HTML changes (processed_date|upload_id, group_key, page, limit)
 *   GET /api/stats            — aggregate counts for a processed_date or upload
 *   GET /api/date-annotations?date= — get note for a processed date
 *   POST /api/date-annotations      — save/delete note for a processed date
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

    // Schema migration — idempotent; silently ignored once column exists
    try {
      await env.DB.prepare("ALTER TABLE uploads ADD COLUMN processed_date TEXT").run();
    } catch (_) {}
    try {
      await env.DB.prepare("CREATE TABLE IF NOT EXISTS date_annotations (processed_date TEXT PRIMARY KEY, annotation TEXT NOT NULL, updated_at TEXT DEFAULT (datetime('now')))").run();
    } catch (_) {}

    // ── /api/uploads ──────────────────────────────────────────────────────────
    if (pathname === "/api/uploads") {
      const limit = Math.min(parseInt(p.limit) || 50, 100);
      const { results } = await env.DB.prepare(
        "SELECT id, created_at, csv_file, html_file, csv_rows, html_items, processed_date FROM uploads ORDER BY created_at DESC LIMIT ?"
      ).bind(limit).all();
      return json({ uploads: results });
    }

    // ── /api/dates ────────────────────────────────────────────────────────────
    if (pathname === "/api/dates") {
      const { results } = await env.DB.prepare(`
        SELECT
          u.processed_date,
          SUM(u.csv_rows)   AS csv_rows,
          SUM(u.html_items) AS html_items,
          COUNT(u.id)       AS upload_count,
          (SELECT html_file FROM uploads
           WHERE processed_date = u.processed_date AND html_file != ''
           ORDER BY created_at DESC LIMIT 1) AS html_file,
          COUNT(r.id) AS total_records,
          COUNT(CASE WHEN r.extraction_status = 'Success' THEN 1 END)         AS extraction_success,
          COUNT(CASE WHEN r.extraction_status != 'Success'
                      AND r.extraction_status != '' THEN 1 END)               AS extraction_failed,
          COUNT(CASE WHEN r.has_plans_specs = 'Yes'
                       OR r.has_bidding_docs = 'Yes' THEN 1 END)              AS pdf_docs_found,
          COUNT(CASE WHEN r.is_project = 'True' THEN 1 END)                   AS approved_count,
          (SELECT hc.group_key || ':' || COUNT(*)
           FROM html_changes hc
           WHERE hc.upload_id IN (
             SELECT id FROM uploads WHERE processed_date = u.processed_date
           ) AND hc.group_key != ''
           GROUP BY hc.group_key
           ORDER BY COUNT(*) DESC LIMIT 1) AS top_html_group,
          (SELECT r2.state || ':' || COUNT(*)
           FROM csv_records r2
           WHERE r2.upload_id IN (
             SELECT id FROM uploads WHERE processed_date = u.processed_date
           ) AND r2.state != ''
           GROUP BY r2.state
           ORDER BY COUNT(*) DESC LIMIT 1) AS top_csv_state
        FROM uploads u
        LEFT JOIN csv_records r ON r.upload_id = u.id
        WHERE u.processed_date IS NOT NULL AND u.processed_date != ''
        GROUP BY u.processed_date
        ORDER BY u.processed_date DESC
        LIMIT 90
      `).all();
      return json({ dates: results });
    }

    // ── DELETE /api/dates/:date/csv — delete only CSV records for a date ─────
    if (request.method === "DELETE" && pathname.match(/^\/api\/dates\/\d{4}-\d{2}-\d{2}\/csv$/)) {
      const date = pathname.split("/")[3];
      const { results: ups } = await env.DB.prepare(
        "SELECT id FROM uploads WHERE processed_date = ?"
      ).bind(date).all();
      for (const u of ups) {
        await env.DB.prepare("DELETE FROM csv_records WHERE upload_id = ?").bind(u.id).run();
        await env.DB.prepare("UPDATE uploads SET csv_rows = 0 WHERE id = ?").bind(u.id).run();
      }
      return json({ ok: true, date, uploads_cleared: ups.length });
    }

    // ── DELETE /api/dates/:date — delete all uploads + records + HTML for a date
    if (request.method === "DELETE" && pathname.match(/^\/api\/dates\/\d{4}-\d{2}-\d{2}$/)) {
      const date = pathname.split("/")[3];
      const { results: ups } = await env.DB.prepare(
        "SELECT id, html_file FROM uploads WHERE processed_date = ?"
      ).bind(date).all();
      for (const u of ups) {
        await Promise.all([
          env.DB.prepare("DELETE FROM csv_records  WHERE upload_id = ?").bind(u.id).run(),
          env.DB.prepare("DELETE FROM html_changes WHERE upload_id = ?").bind(u.id).run(),
        ]);
        if (u.html_file) await env.HTML_BUCKET.delete(u.html_file).catch(() => {});
        await env.DB.prepare("DELETE FROM uploads WHERE id = ?").bind(u.id).run();
      }
      await env.DB.prepare("DELETE FROM date_annotations WHERE processed_date = ?").bind(date).run().catch(() => {});
      return json({ ok: true, date, deleted_uploads: ups.length });
    }

    // ── DELETE /api/uploads/:id/csv ──────────────────────────────────────────
    if (request.method === "DELETE" && pathname.match(/^\/api\/uploads\/[^/]+\/csv$/)) {
      const uploadId = pathname.split("/")[3];
      await env.DB.prepare("DELETE FROM csv_records WHERE upload_id = ?").bind(uploadId).run();
      await env.DB.prepare("UPDATE uploads SET csv_rows = 0 WHERE id = ?").bind(uploadId).run();
      return json({ ok: true, deleted_csv_for: uploadId });
    }

    // ── GET /api/annotations?date=YYYY-MM-DD ─────────────────────────────────
    if (pathname === "/api/annotations" && request.method === "GET") {
      await env.DB.prepare(
        "CREATE TABLE IF NOT EXISTS date_url_annotations (processed_date TEXT NOT NULL, url_key TEXT NOT NULL, annotation TEXT NOT NULL, updated_at TEXT DEFAULT (datetime('now')), PRIMARY KEY (processed_date, url_key))"
      ).run();
      const date = (p.date || "").trim();
      if (!date) return json({ annotations: {} });
      const { results } = await env.DB.prepare(
        "SELECT url_key, annotation FROM date_url_annotations WHERE processed_date = ?"
      ).bind(date).all();
      const annotations = {};
      for (const row of results) annotations[row.url_key] = row.annotation;
      return json({ annotations });
    }

    // ── POST /api/annotations { processed_date, url_key, annotation } ─────────
    if (pathname === "/api/annotations" && request.method === "POST") {
      const body       = await request.json();
      const date       = (body.processed_date || "").trim();
      const urlKey     = (body.url_key        || "").trim();
      const annotation = (body.annotation     || "").trim();
      if (!date)   return err("processed_date required");
      if (!urlKey) return err("url_key required");
      await env.DB.prepare(
        "CREATE TABLE IF NOT EXISTS date_url_annotations (processed_date TEXT NOT NULL, url_key TEXT NOT NULL, annotation TEXT NOT NULL, updated_at TEXT DEFAULT (datetime('now')), PRIMARY KEY (processed_date, url_key))"
      ).run();
      if (annotation) {
        await env.DB.prepare(
          "INSERT OR REPLACE INTO date_url_annotations (processed_date, url_key, annotation, updated_at) VALUES (?, ?, ?, datetime('now'))"
        ).bind(date, urlKey, annotation).run();
      } else {
        await env.DB.prepare(
          "DELETE FROM date_url_annotations WHERE processed_date = ? AND url_key = ?"
        ).bind(date, urlKey).run();
      }
      return json({ ok: true });
    }

    // ── DELETE /api/uploads/:id ───────────────────────────────────────────────
    if (request.method === "DELETE" && pathname.match(/^\/api\/uploads\/[^/]+$/)) {
      const uploadId = pathname.split("/").pop();
      // Fetch html_file key before deleting so we can clean R2 too
      const uploadRow = await env.DB.prepare("SELECT html_file FROM uploads WHERE id = ?").bind(uploadId).first();
      await Promise.all([
        env.DB.prepare("DELETE FROM csv_records WHERE upload_id = ?").bind(uploadId).run(),
        env.DB.prepare("DELETE FROM html_changes WHERE upload_id = ?").bind(uploadId).run(),
        env.DB.prepare("DELETE FROM html_annotations WHERE upload_id = ?").bind(uploadId).run().catch(() => {}),
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

      // resolve upload_ids: processed_date > upload_id > latest date
      let uploadIds;
      if (p.processed_date) {
        const { results: rows } = await env.DB.prepare(
          "SELECT id FROM uploads WHERE processed_date = ?"
        ).bind(p.processed_date).all();
        uploadIds = rows.map(r => r.id);
        if (!uploadIds.length) return json({ records: [], total: 0, page, limit });
      } else if (p.upload_id) {
        uploadIds = [p.upload_id];
      } else {
        const row = await env.DB.prepare(
          "SELECT processed_date FROM uploads WHERE processed_date IS NOT NULL AND processed_date != '' ORDER BY processed_date DESC LIMIT 1"
        ).first();
        if (!row) {
          const latest = await env.DB.prepare("SELECT id FROM uploads ORDER BY created_at DESC LIMIT 1").first();
          if (!latest) return json({ records: [], total: 0, page, limit });
          uploadIds = [latest.id];
        } else {
          const { results: rows } = await env.DB.prepare(
            "SELECT id FROM uploads WHERE processed_date = ?"
          ).bind(row.processed_date).all();
          uploadIds = rows.map(r => r.id);
        }
      }

      const idPH = uploadIds.map(() => "?").join(",");
      let whereClauses = [`upload_id IN (${idPH})`];
      let binds = [...uploadIds];

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
        processed_date: p.processed_date || null,
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

      let uploadIds;
      if (p.processed_date) {
        const { results: rows } = await env.DB.prepare(
          "SELECT id FROM uploads WHERE processed_date = ?"
        ).bind(p.processed_date).all();
        uploadIds = rows.map(r => r.id);
        if (!uploadIds.length) return json({ changes: [], total: 0, page, limit });
      } else if (p.upload_id) {
        uploadIds = [p.upload_id];
      } else {
        const row = await env.DB.prepare(
          "SELECT processed_date FROM uploads WHERE processed_date IS NOT NULL AND processed_date != '' ORDER BY processed_date DESC LIMIT 1"
        ).first();
        if (!row) {
          const latest = await env.DB.prepare("SELECT id FROM uploads ORDER BY created_at DESC LIMIT 1").first();
          if (!latest) return json({ changes: [], total: 0, page, limit });
          uploadIds = [latest.id];
        } else {
          const { results: rows } = await env.DB.prepare(
            "SELECT id FROM uploads WHERE processed_date = ?"
          ).bind(row.processed_date).all();
          uploadIds = rows.map(r => r.id);
        }
      }

      const htmlIdPH = uploadIds.map(() => "?").join(",");
      let whereClauses = [`upload_id IN (${htmlIdPH})`];
      let binds = [...uploadIds];

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

      // distinct group_keys across all upload_ids for this date
      const { results: groups } = await env.DB.prepare(
        `SELECT DISTINCT group_key FROM html_changes WHERE upload_id IN (${htmlIdPH}) ORDER BY group_key`
      ).bind(...uploadIds).all();

      return json({
        processed_date: p.processed_date || null,
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
      let uploadIds;
      if (p.processed_date) {
        const { results: rows } = await env.DB.prepare(
          "SELECT id FROM uploads WHERE processed_date = ?"
        ).bind(p.processed_date).all();
        uploadIds = rows.map(r => r.id);
        if (!uploadIds.length) return json({ error: "No data for this date" }, 404);
      } else if (p.upload_id) {
        uploadIds = [p.upload_id];
      } else {
        const row = await env.DB.prepare(
          "SELECT processed_date FROM uploads WHERE processed_date IS NOT NULL AND processed_date != '' ORDER BY processed_date DESC LIMIT 1"
        ).first();
        if (!row) {
          const latest = await env.DB.prepare("SELECT id FROM uploads ORDER BY created_at DESC LIMIT 1").first();
          if (!latest) return json({ error: "No uploads found" }, 404);
          uploadIds = [latest.id];
        } else {
          const { results: rows } = await env.DB.prepare(
            "SELECT id FROM uploads WHERE processed_date = ?"
          ).bind(row.processed_date).all();
          uploadIds = rows.map(r => r.id);
        }
      }
      const uploadId = uploadIds[0]; // for backward-compat stats queries below

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
      const dateStr  = ((form.get("processed_date") || "").trim().slice(0, 10))
                    || now.toISOString().slice(0, 10);
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

    // ── /api/html-changes/import (POST) — bulk-insert parsed HTML items into D1
    if (pathname === "/api/html-changes/import" && request.method === "POST") {
      const body     = await request.json();
      const uploadId = (body.upload_id || "").trim();
      const items    = body.items || [];
      if (!uploadId) return err("upload_id required");

      const cols  = ["upload_id","title","date","url","change_summary","changes_full_html","path","group_key"];
      const phRow = "(" + cols.map(() => "?").join(",") + ")";
      const CHUNK = 10; // 10 rows × 8 cols = 80 params — under D1 limit

      // Build all statements upfront, then execute in one batch round-trip
      const statements = [
        env.DB.prepare("DELETE FROM html_changes WHERE upload_id = ?").bind(uploadId),
      ];

      for (let i = 0; i < items.length; i += CHUNK) {
        const chunk  = items.slice(i, i + CHUNK);
        const sql    = `INSERT INTO html_changes (${cols.join(",")}) VALUES ${chunk.map(() => phRow).join(",")}`;
        const params = chunk.flatMap(it => [
          uploadId,
          (it.title           || "").slice(0, 500),
          (it.date            || "").slice(0, 100),
          (it.url             || "").slice(0, 500),
          (it.changeSummary   || "").slice(0, 1000),
          (it.changesFullHTML || "").slice(0, 4000),
          (it.path            || "").slice(0, 500),
          (it.groupKey        || "").slice(0, 100),
        ]);
        statements.push(env.DB.prepare(sql).bind(...params));
      }

      statements.push(
        env.DB.prepare("UPDATE uploads SET html_items = ? WHERE id = ?").bind(items.length, uploadId)
      );

      await env.DB.batch(statements);
      return json({ ok: true, inserted: items.length });
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

    // ── GET /api/date-annotations?date=YYYY-MM-DD ─────────────────────────────
    if (pathname === "/api/date-annotations" && request.method === "GET") {
      await env.DB.prepare(
        "CREATE TABLE IF NOT EXISTS date_annotations (processed_date TEXT PRIMARY KEY, annotation TEXT NOT NULL, updated_at TEXT DEFAULT (datetime('now')))"
      ).run();
      const date = (p.date || "").trim();
      if (!date) return json({ annotation: "" });
      const row = await env.DB.prepare(
        "SELECT annotation FROM date_annotations WHERE processed_date = ?"
      ).bind(date).first();
      return json({ annotation: row?.annotation || "" });
    }

    // ── POST /api/date-annotations { date, annotation } ──────────────────────
    if (pathname === "/api/date-annotations" && request.method === "POST") {
      await env.DB.prepare(
        "CREATE TABLE IF NOT EXISTS date_annotations (processed_date TEXT PRIMARY KEY, annotation TEXT NOT NULL, updated_at TEXT DEFAULT (datetime('now')))"
      ).run();
      const body       = await request.json();
      const date       = (body.date       || "").trim();
      const annotation = (body.annotation || "").trim();
      if (!date) return err("date required");
      if (annotation) {
        await env.DB.prepare(
          "INSERT OR REPLACE INTO date_annotations (processed_date, annotation, updated_at) VALUES (?, ?, datetime('now'))"
        ).bind(date, annotation).run();
      } else {
        await env.DB.prepare(
          "DELETE FROM date_annotations WHERE processed_date = ?"
        ).bind(date).run();
      }
      return json({ ok: true });
    }

    // ── POST /api/migrate/backfill-dates ──────────────────────────────────────
    if (pathname === "/api/migrate/backfill-dates" && request.method === "POST") {
      let changes = 0;

      // Step 1 — set processed_date from csv_records.processing_date where we can
      const r1 = await env.DB.prepare(`
        UPDATE uploads
        SET processed_date = (
          SELECT processing_date FROM csv_records
          WHERE csv_records.upload_id = uploads.id
            AND processing_date IS NOT NULL AND processing_date != ''
          LIMIT 1
        )
        WHERE (processed_date IS NULL OR processed_date = '')
          AND EXISTS (
            SELECT 1 FROM csv_records
            WHERE csv_records.upload_id = uploads.id
              AND processing_date IS NOT NULL AND processing_date != ''
          )
      `).run();
      changes += r1.meta?.changes ?? 0;

      // Step 2 — for uploads still missing processed_date, try html_file R2 key prefix
      const r2a = await env.DB.prepare(`
        UPDATE uploads
        SET processed_date = substr(html_file, 1, 10)
        WHERE (processed_date IS NULL OR processed_date = '')
          AND html_file GLOB '????-??-??/*'
      `).run();
      changes += r2a.meta?.changes ?? 0;

      // Step 3 — list R2 objects and link any unlinked uploads by matching date
      try {
        const listed = await env.HTML_BUCKET.list({ limit: 500 });
        for (const obj of listed.objects) {
          const m = obj.key.match(/^(\d{4}-\d{2}-\d{2})\//);
          if (!m) continue;
          const date = m[1];
          // Find uploads for this date that have no html_file set
          const { results: rows } = await env.DB.prepare(`
            SELECT DISTINCT u.id FROM uploads u
            LEFT JOIN csv_records r ON r.upload_id = u.id
            WHERE u.processed_date = ?
              AND (u.html_file IS NULL OR u.html_file = '')
            LIMIT 1
          `).bind(date).all();
          for (const row of rows) {
            await env.DB.prepare(
              "UPDATE uploads SET html_file = ? WHERE id = ?"
            ).bind(obj.key, row.id).run();
            changes++;
          }
          // Also find uploads whose csv_records.processing_date matches but processed_date still null
          const { results: unlinked } = await env.DB.prepare(`
            SELECT DISTINCT u.id FROM uploads u
            JOIN csv_records r ON r.upload_id = u.id
            WHERE r.processing_date = ?
              AND (u.processed_date IS NULL OR u.processed_date = '')
            LIMIT 1
          `).bind(date).all();
          for (const row of unlinked) {
            await env.DB.prepare(
              "UPDATE uploads SET processed_date = ?, html_file = CASE WHEN html_file = '' OR html_file IS NULL THEN ? ELSE html_file END WHERE id = ?"
            ).bind(date, obj.key, row.id).run();
            changes++;
          }
        }
      } catch (_) {}

      return json({ ok: true, changes });
    }

    // ── POST /api/html-link — persist R2 key association for a date ──────────
    if (pathname === "/api/html-link" && request.method === "POST") {
      const body = await request.json();
      const date = (body.processed_date || "").trim();
      const key  = (body.key            || "").trim();
      if (!date || !key) return err("processed_date and key required");
      await env.DB.prepare(
        "UPDATE uploads SET html_file = ? WHERE processed_date = ? AND (html_file IS NULL OR html_file = '')"
      ).bind(key, date).run();
      return json({ ok: true });
    }

    return err("Not found", 404);
  },
};
