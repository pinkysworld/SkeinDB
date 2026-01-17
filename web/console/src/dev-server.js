// Minimal dev server placeholder.
// Replace with Vite/React/Svelte later.
import http from 'http';
import fs from 'fs';
import path from 'path';

const port = Number(process.env.PORT || 5173);
const root = path.resolve(process.cwd());

const server = http.createServer((req, res) => {
  const url = req.url || '/';
  const filePath = url === '/' ? path.join(root, 'src', 'index.html') : path.join(root, url);

  fs.readFile(filePath, (err, data) => {
    if (err) {
      res.writeHead(404, { 'Content-Type': 'text/plain' });
      res.end('Not found');
      return;
    }
    const contentType = filePath.endsWith('.js') ? 'text/javascript' : 'text/html';
    res.writeHead(200, { 'Content-Type': contentType });
    res.end(data);
  });
});

server.listen(port, () => {
  console.log(`SkeinConsole dev server running at http://127.0.0.1:${port}/`);
});
