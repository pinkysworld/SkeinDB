// Placeholder build: copy src/index.html to dist/
import fs from 'fs';
import path from 'path';

const root = path.resolve(process.cwd());
const dist = path.join(root, 'dist');
fs.rmSync(dist, { recursive: true, force: true });
fs.mkdirSync(dist, { recursive: true });

fs.copyFileSync(path.join(root, 'src', 'index.html'), path.join(dist, 'index.html'));
console.log('Built web console placeholder to web/console/dist');
