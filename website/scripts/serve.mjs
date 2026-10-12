import http from "node:http";
import fs from "node:fs/promises";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { base } from "../src/templates.mjs";
const root = path.resolve(fileURLToPath(new URL("../dist/", import.meta.url)));
const types = {
  ".html": "text/html; charset=utf-8",
  ".css": "text/css",
  ".js": "text/javascript",
  ".json": "application/json",
  ".webp": "image/webp",
  ".svg": "image/svg+xml",
  ".woff2": "font/woff2",
  ".xml": "application/xml",
  ".txt": "text/plain",
};
const port = Number(process.env.PORT || 4173);
http
  .createServer(async (req, res) => {
    try {
      const pathname = decodeURIComponent(
        new URL(req.url, "http://localhost").pathname,
      );
      if (pathname === "/") {
        res.writeHead(302, { Location: base });
        res.end();
        return;
      }
      if (!pathname.startsWith(base)) throw Error();
      let file = path.resolve(root, pathname.slice(base.length));
      if (file !== root && !file.startsWith(root + path.sep)) throw Error();
      const stat = await fs.stat(file);
      if (stat.isDirectory()) {
        if (!pathname.endsWith("/")) {
          res.writeHead(301, { Location: pathname + "/" });
          res.end();
          return;
        }
        file = path.join(file, "index.html");
      }
      const content = await fs.readFile(file);
      res.writeHead(200, {
        "Content-Type": types[path.extname(file)] || "application/octet-stream",
      });
      res.end(content);
    } catch {
      res.writeHead(404, { "Content-Type": "text/html; charset=utf-8" });
      res.end(await fs.readFile(path.join(root, "404.html")));
    }
  })
  .listen(port, "127.0.0.1", () =>
    console.log(`KUI microsite: http://localhost:${port}${base}`),
  );
