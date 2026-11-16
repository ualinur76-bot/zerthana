// Ортақ дерекқор: Upstash Redis (Vercel Storage) арқылы құжаттарды сақтау.
// Кілттер Vercel-дің өзі қосқан KV_REST_API_URL және KV_REST_API_TOKEN айнымалыларынан алынады.
const URL_ = process.env.KV_REST_API_URL || process.env.UPSTASH_REDIS_REST_URL;
const TOKEN = process.env.KV_REST_API_TOKEN || process.env.UPSTASH_REDIS_REST_TOKEN;
const P = 'ez:';
const PATH_RE = /^[A-Za-z0-9_\-]{1,64}(\/[A-Za-z0-9_\-.@]{1,200})+$/;
const COLL_RE = /^[A-Za-z0-9_\-]{1,64}(\/[A-Za-z0-9_\-.@]{1,200}\/[A-Za-z0-9_\-]{1,64})*$/;

async function redis(cmds) {
  const r = await fetch(URL_ + '/pipeline', {
    method: 'POST',
    headers: { Authorization: 'Bearer ' + TOKEN, 'Content-Type': 'application/json' },
    body: JSON.stringify(cmds)
  });
  if (!r.ok) throw { code: 'unavailable', message: 'Redis ' + r.status };
  const out = await r.json();
  return out.map(x => { if (x.error) throw { code: 'unavailable', message: x.error }; return x.result; });
}
const split = path => { const i = path.lastIndexOf('/'); return [path.slice(0, i), path.slice(i + 1)]; };
const parse = s => { try { return s ? JSON.parse(s) : null; } catch (e) { return null; } };
function deepMerge(a, b) {
  const out = Object.assign({}, a);
  Object.keys(b || {}).forEach(k => {
    const v = b[k];
    out[k] = v && typeof v === 'object' && !Array.isArray(v) && a && a[k] && typeof a[k] === 'object' && !Array.isArray(a[k]) ? deepMerge(a[k], v) : v;
  });
  return out;
}
function match(d, where) {
  return (where || []).every(([f, op, v]) => {
    const x = d ? d[f] : undefined;
    switch (op) {
      case '==': return x === v;
      case '!=': return x !== v;
      case 'in': return Array.isArray(v) && v.includes(x);
      case 'not-in': return Array.isArray(v) && !v.includes(x);
      case 'array-contains': return Array.isArray(x) && x.includes(v);
      case '<': return x < v;
      case '<=': return x <= v;
      case '>': return x > v;
      case '>=': return x >= v;
      default: return true;
    }
  });
}

module.exports = async (req, res) => {
  res.setHeader('Cache-Control', 'no-store');
  if (req.method !== 'POST') return res.status(405).json({ error: { code: 'invalid_argument', message: 'POST only' } });
  if (!URL_ || !TOKEN) return res.status(503).json({ error: { code: 'unavailable', message: 'Дерекқор кілттері орнатылмаған' } });
  try {
    let b = req.body;
    if (typeof b === 'string') b = JSON.parse(b);
    const { op, path, coll, data, where } = b || {};
    if (op === 'ping') { await redis([['PING']]); return res.json({ ok: true }); }
    if (op === 'list') {
      if (!COLL_RE.test(coll || '')) throw { code: 'invalid_argument', message: 'Қате жинақ атауы' };
      const [ids] = await redis([['SMEMBERS', P + 'coll:' + coll]]);
      if (!ids || !ids.length) return res.json({ docs: [] });
      const docs = [];
      for (let i = 0; i < ids.length; i += 200) {
        const part = ids.slice(i, i + 200);
        const [vals] = await redis([['MGET', ...part.map(id => P + 'doc:' + coll + '/' + id)]]);
        vals.forEach((s, j) => { const d = parse(s); if (d && match(d, where)) docs.push({ id: part[j], data: d }); });
      }
      return res.json({ docs });
    }
    if (!PATH_RE.test(path || '')) throw { code: 'invalid_argument', message: 'Қате құжат жолы' };
    const [c, id] = split(path), key = P + 'doc:' + path, ckey = P + 'coll:' + c;
    if (op === 'get') { const [s] = await redis([['GET', key]]); return res.json({ data: parse(s) }); }
    if (op === 'set' || op === 'update') {
      if (!data || typeof data !== 'object') throw { code: 'invalid_argument', message: 'Деректер жоқ' };
      let doc = data;
      if (op === 'update') {
        const [s] = await redis([['GET', key]]); const cur = parse(s);
        if (!cur) throw { code: 'invalid_argument', message: 'Құжат табылмады' };
        doc = deepMerge(cur, data);
      }
      const str = JSON.stringify(doc);
      if (str.length > 900000) throw { code: 'quota_exceeded', message: 'Құжат тым үлкен' };
      await redis([['SET', key, str], ['SADD', ckey, id]]);
      return res.json({ ok: true });
    }
    if (op === 'delete') { await redis([['DEL', key], ['SREM', ckey, id]]); return res.json({ ok: true }); }
    throw { code: 'invalid_argument', message: 'Белгісіз операция' };
  } catch (e) {
    const code = (e && e.code) || 'unavailable';
    return res.status(code === 'invalid_argument' ? 400 : 500).json({ error: { code, message: (e && e.message) || String(e) } });
  }
};
