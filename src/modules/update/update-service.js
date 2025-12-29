const fs = require('fs');
const path = require('path');
const crypto = require('crypto');

const UPDATES_DIR = process.env.UPDATES_DIR || 'D:\\deploy\\pps_updates';
const UPDATE_TOKEN = process.env.UPDATE_TOKEN || '';

// whitelist appId
const ALLOWED_APPS = new Set(['tablet', 'mobile']);

function assertAppId(appId) {
  const id = String(appId || '').trim().toLowerCase();
  if (!ALLOWED_APPS.has(id)) {
    const e = new Error('Invalid appId (use: tablet/mobile)');
    e.statusCode = 400;
    throw e;
  }
  return id;
}

function safeJoin(baseDir, fileName) {
  const cleaned = String(fileName || '').replace(/[/\\]+/g, path.sep);
  const resolved = path.resolve(baseDir, cleaned);
  const baseResolved = path.resolve(baseDir);

  if (!resolved.startsWith(baseResolved + path.sep) && resolved !== baseResolved) {
    const e = new Error('Invalid path');
    e.statusCode = 400;
    throw e;
  }
  return resolved;
}

function ensureExists(filePath, msg = 'File not found') {
  if (!fs.existsSync(filePath)) {
    const e = new Error(msg);
    e.statusCode = 404;
    throw e;
  }
}

async function sha256File(filePath) {
  return await new Promise((resolve, reject) => {
    const hash = crypto.createHash('sha256');
    const s = fs.createReadStream(filePath);
    s.on('data', (d) => hash.update(d));
    s.on('end', () => resolve(hash.digest('hex')));
    s.on('error', reject);
  });
}

function appBaseDir(appId) {
  const id = assertAppId(appId);
  return safeJoin(UPDATES_DIR, id); // D:\deploy\pps_updates\tablet  / mobile
}

async function readVersionJson(appId) {
  const base = appBaseDir(appId);
  const verPath = safeJoin(base, 'version.json');
  ensureExists(verPath, 'version.json not found');

  const raw = fs.readFileSync(verPath, 'utf8');
  let data;
  try {
    data = JSON.parse(raw);
  } catch {
    const e = new Error('Invalid JSON in version.json');
    e.statusCode = 500;
    throw e;
  }

  if (!data.latestVersion || !data.fileName) {
    const e = new Error('version.json missing latestVersion/fileName');
    e.statusCode = 500;
    throw e;
  }

  return data;
}

async function streamApk(appId, fileName, req, res) {
  if (!fileName.toLowerCase().endsWith('.apk')) {
    const e = new Error('Invalid file (must be .apk)');
    e.statusCode = 400;
    throw e;
  }

  const base = appBaseDir(appId);
  const filePath = safeJoin(base, fileName);
  ensureExists(filePath, 'APK not found');

  const stat = fs.statSync(filePath);
  const size = stat.size;
  const range = req.headers.range;

  res.setHeader('Content-Type', 'application/vnd.android.package-archive');
  res.setHeader('Accept-Ranges', 'bytes');
  res.setHeader('Cache-Control', 'no-store');
  res.setHeader('Content-Disposition', `attachment; filename="${fileName}"`);

  if (range) {
    const match = /^bytes=(\d+)-(\d*)$/.exec(range);
    if (!match) return res.status(416).end();

    const start = parseInt(match[1], 10);
    const end = match[2] ? parseInt(match[2], 10) : size - 1;

    if (start >= size || end >= size || start > end) return res.status(416).end();

    res.status(206);
    res.setHeader('Content-Range', `bytes ${start}-${end}/${size}`);
    res.setHeader('Content-Length', end - start + 1);

    fs.createReadStream(filePath, { start, end }).pipe(res);
    return;
  }

  res.setHeader('Content-Length', size);
  fs.createReadStream(filePath).pipe(res);
}

async function assertPublishToken(token) {
  if (!UPDATE_TOKEN) {
    const e = new Error('Publish disabled (UPDATE_TOKEN empty)');
    e.statusCode = 403;
    throw e;
  }
  if (!token || token !== UPDATE_TOKEN) {
    const e = new Error('Invalid publish token');
    e.statusCode = 401;
    throw e;
  }
}

async function publishVersionJson(appId, body) {
  const id = assertAppId(appId);

  const latestVersion = String(body.latestVersion || '').trim();
  const minVersion = String(body.minVersion || '0.0.0').trim();
  const forceUpdate = Boolean(body.forceUpdate);
  const fileName = String(body.fileName || '').trim();
  const changelog = String(body.changelog || '').trim();

  if (!latestVersion) {
    const e = new Error('latestVersion required');
    e.statusCode = 400;
    throw e;
  }
  if (!fileName.toLowerCase().endsWith('.apk')) {
    const e = new Error('fileName must be .apk');
    e.statusCode = 400;
    throw e;
  }

  const base = appBaseDir(id);
  const apkPath = safeJoin(base, fileName);
  ensureExists(apkPath, 'APK not found in updates dir');

  const sha256 = await sha256File(apkPath);

  const payload = {
    latestVersion,
    minVersion,
    forceUpdate,
    fileName,
    sha256,
    changelog,
    updatedAt: new Date().toISOString(),
    appId: id,
  };

  const verPath = safeJoin(base, 'version.json');
  fs.writeFileSync(verPath, JSON.stringify(payload, null, 2));
  return payload;
}

module.exports = {
  readVersionJson,
  streamApk,
  assertPublishToken,
  publishVersionJson,
};
