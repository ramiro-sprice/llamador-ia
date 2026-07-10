import 'dotenv/config';
import crypto from 'node:crypto';
import http from 'node:http';
import path from 'node:path';
import { fileURLToPath } from 'node:url';
import express from 'express';
import OpenAI from 'openai';
import twilio from 'twilio';
import { WebSocketServer } from 'ws';
import { createContact, databaseConfigured, initializeDatabase, listContacts, saveCallProgress, saveCallStart, updateContact } from './database.js';

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const app = express();
app.set('trust proxy', 1);
const server = http.createServer(app);
const wss = new WebSocketServer({ noServer: true });
const port = Number(process.env.PORT || 3000);
const publicUrl = (process.env.PUBLIC_URL || '').replace(/\/$/, '');
const model = process.env.OPENAI_MODEL || 'gpt-5.4-mini';
const maxCallSeconds = Math.min(Number(process.env.MAX_CALL_SECONDS || 300), 900);
const openai = process.env.OPENAI_API_KEY ? new OpenAI({ apiKey: process.env.OPENAI_API_KEY }) : null;
const RATE_WINDOW_MS = 10 * 60 * 1000;
const RATE_MAX_CALLS = 5;
const NUMBER_COOLDOWN_MS = 60 * 1000;

app.use(express.json());
app.use(express.urlencoded({ extended: false }));
app.use(express.static(path.join(__dirname, '..', 'public')));

const calls = new Map();
const attemptsByIp = new Map();
const lastCallByNumber = new Map();

function argentinaNumber(value) {
  return /^\+54\d{10,13}$/.test(String(value || '').replace(/[\s()-]/g, ''));
}

function requireConfig(names) {
  const missing = names.filter((name) => !process.env[name]);
  if (missing.length) throw new Error(`Falta configurar: ${missing.join(', ')}`);
}

function validAdminToken(value) {
  const expected = String(process.env.CALL_ADMIN_TOKEN || '').trim();
  const supplied = String(value || '').trim();
  if (expected.length < 16 || supplied.length !== expected.length) return false;
  return crypto.timingSafeEqual(Buffer.from(supplied), Buffer.from(expected));
}

function adminTokenFrom(req) {
  return req.get('x-admin-token') || req.body?.adminToken || '';
}

function safeCallError(error) {
  const code = error?.code;
  if (error?.status === 401) return 'OpenAI rechazó la credencial configurada.';
  if (error?.status === 429) return 'OpenAI no tiene cuota disponible o alcanzó su límite.';
  if (code === 21215 || code === 21219) return 'Twilio no permite usar el número emisor configurado.';
  if (code === 21211) return 'El número destinatario no es válido para Twilio.';
  if (code === 21408) return 'Twilio no tiene habilitadas las llamadas hacia Argentina.';
  return 'El proveedor telefónico rechazó la llamada. Revisá los registros de Twilio.';
}

function rateLimit(req, to) {
  const now = Date.now();
  const ip = req.ip || req.socket.remoteAddress || 'unknown';
  const recent = (attemptsByIp.get(ip) || []).filter((time) => now - time < RATE_WINDOW_MS);
  if (recent.length >= RATE_MAX_CALLS) return 'Alcanzaste el límite de cinco llamadas cada diez minutos.';
  if (now - (lastCallByNumber.get(to) || 0) < NUMBER_COOLDOWN_MS) return 'Esperá un minuto antes de volver a llamar al mismo número.';
  recent.push(now);
  attemptsByIp.set(ip, recent);
  lastCallByNumber.set(to, now);
  return null;
}

function publicCall(reference, call) {
  return {
    reference,
    status: call.status,
    createdAt: call.createdAt,
    updatedAt: call.updatedAt || call.createdAt,
    hasError: Boolean(call.error),
    error: call.error || null,
    conversationTurns: call.transcript?.length || 0,
  };
}

app.get('/api/health', (_req, res) => {
  res.json({
    ok: true,
    mode: process.env.TWILIO_ACCOUNT_SID ? 'configured' : 'simulation-only',
    adminConfigured: String(process.env.CALL_ADMIN_TOKEN || '').trim().length >= 16,
    databaseConfigured: databaseConfigured(),
  });
});

app.get('/api/contacts', async (req, res) => {
  if (!validAdminToken(adminTokenFrom(req))) return res.status(403).json({ error: 'No autorizado.' });
  if (!databaseConfigured()) return res.status(503).json({ error: 'La base de datos todavía no está configurada.' });
  try { res.json({ contacts: await listContacts() }); }
  catch { res.status(500).json({ error: 'No se pudieron cargar los contactos.' }); }
});

app.post('/api/contacts', async (req, res) => {
  if (!validAdminToken(adminTokenFrom(req))) return res.status(403).json({ error: 'No autorizado.' });
  if (!databaseConfigured()) return res.status(503).json({ error: 'La base de datos todavía no está configurada.' });
  const phone = String(req.body.phone || '').replace(/[\s()-]/g, '');
  if (!argentinaNumber(phone)) return res.status(400).json({ error: 'El teléfono debe estar en formato internacional argentino +54.' });
  try {
    const contact = await createContact({ ...req.body, id: crypto.randomUUID(), phone });
    res.status(201).json({ contact });
  } catch (error) {
    if (error?.code === '23505') return res.status(409).json({ error: 'Ese teléfono ya existe en la base.' });
    res.status(500).json({ error: 'No se pudo guardar el contacto.' });
  }
});

app.patch('/api/contacts/:id', async (req, res) => {
  if (!validAdminToken(adminTokenFrom(req))) return res.status(403).json({ error: 'No autorizado.' });
  if (!databaseConfigured()) return res.status(503).json({ error: 'La base de datos todavía no está configurada.' });
  try {
    const contact = await updateContact(req.params.id, req.body);
    if (!contact) return res.status(404).json({ error: 'Contacto no encontrado o sin cambios.' });
    res.json({ contact });
  } catch { res.status(500).json({ error: 'No se pudo actualizar el contacto.' }); }
});

app.post('/api/calls', async (req, res) => {
  const to = String(req.body.to || '').replace(/[\s()-]/g, '');
  const fixedMessage = String(req.body.fixedMessage || '').trim();
  const instructions = String(req.body.instructions || '').trim();
  const contactId = String(req.body.contactId || '').trim() || null;
  const dryRun = req.body.dryRun !== false;

  if (!argentinaNumber(to)) return res.status(400).json({ error: 'Usá formato internacional argentino: +54 seguido del número.' });
  if (!fixedMessage || fixedMessage.length > 1000) return res.status(400).json({ error: 'El mensaje debe tener entre 1 y 1000 caracteres.' });
  if (!instructions || instructions.length > 4000) return res.status(400).json({ error: 'Las instrucciones deben tener entre 1 y 4000 caracteres.' });
  if (!dryRun && !validAdminToken(req.body.adminToken)) {
    return res.status(403).json({ error: 'Clave administrativa incorrecta o no configurada.' });
  }
  if (!dryRun) {
    const limitError = rateLimit(req, to);
    if (limitError) return res.status(429).json({ error: limitError });
  }

  const reference = crypto.randomUUID();
  calls.set(reference, { reference, to, contactId, fixedMessage, instructions, createdAt: Date.now(), updatedAt: Date.now(), status: dryRun ? 'simulated' : 'queued', transcript: [] });
  if (dryRun) return res.json({ ok: true, dryRun: true, reference, message: 'Simulación válida; no se realizó ninguna llamada.' });

  try {
    requireConfig(['PUBLIC_URL', 'TWILIO_ACCOUNT_SID', 'TWILIO_AUTH_TOKEN', 'TWILIO_PHONE_NUMBER', 'OPENAI_API_KEY']);
    const client = twilio(process.env.TWILIO_ACCOUNT_SID, process.env.TWILIO_AUTH_TOKEN);
    const call = await client.calls.create({
      to,
      from: process.env.TWILIO_PHONE_NUMBER,
      url: `${publicUrl}/twilio/voice?reference=${encodeURIComponent(reference)}`,
      statusCallback: `${publicUrl}/twilio/status?reference=${encodeURIComponent(reference)}`,
      statusCallbackEvent: ['initiated', 'ringing', 'answered', 'completed'],
      timeout: 30,
      timeLimit: maxCallSeconds,
    });
    calls.get(reference).sid = call.sid;
    await saveCallStart(reference, contactId, call.sid, 'queued');
    res.json({ ok: true, dryRun: false, reference, sid: call.sid });
  } catch (error) {
    calls.get(reference).status = 'failed';
    calls.get(reference).error = safeCallError(error);
    calls.get(reference).updatedAt = Date.now();
    res.status(502).json({ error: calls.get(reference).error, reference });
  }
});

app.get('/api/calls/:reference', (req, res) => {
  if (!validAdminToken(adminTokenFrom(req))) return res.status(403).json({ error: 'No autorizado.' });
  const call = calls.get(req.params.reference);
  if (!call) return res.status(404).json({ error: 'Llamada no encontrada.' });
  res.json(publicCall(req.params.reference, call));
});

app.post('/twilio/voice', (req, res) => {
  const call = calls.get(String(req.query.reference || ''));
  if (!call) return res.status(404).type('text/xml').send('<Response><Hangup/></Response>');
  const response = new twilio.twiml.VoiceResponse();
  const connect = response.connect();
  const relay = connect.conversationRelay({
    url: `${publicUrl.replace(/^http/, 'ws')}/conversation`,
    welcomeGreeting: call.fixedMessage,
    welcomeGreetingInterruptible: 'none',
    language: 'es-US',
    interruptible: 'speech',
  });
  relay.parameter({ name: 'reference', value: String(req.query.reference) });
  res.type('text/xml').send(response.toString());
});

app.post('/twilio/status', (req, res) => {
  const call = calls.get(String(req.query.reference || ''));
  if (call) {
    call.status = req.body.CallStatus || call.status;
    call.updatedAt = Date.now();
    saveCallProgress(String(req.query.reference || ''), call).catch(() => {});
  }
  res.sendStatus(204);
});

server.on('upgrade', (request, socket, head) => {
  if (new URL(request.url, 'http://localhost').pathname !== '/conversation') return socket.destroy();
  wss.handleUpgrade(request, socket, head, (ws) => wss.emit('connection', ws, request));
});

wss.on('connection', (ws) => {
  let call;
  const history = [];
  let responding = false;
  ws.isAlive = true;
  ws.on('pong', () => { ws.isAlive = true; });

  ws.on('message', async (raw) => {
    let event;
    try { event = JSON.parse(raw.toString()); } catch { return; }
    if (event.type === 'setup') {
      call = calls.get(event.customParameters?.reference);
      if (!call) return ws.close(1008, 'Referencia desconocida');
      call.status = 'in-progress';
      call.updatedAt = Date.now();
      return;
    }
    if (event.type !== 'prompt' || !event.last || !call) return;

    const userText = String(event.voicePrompt || '').trim();
    if (!userText) return;
    if (/\b(no me llamen|no me interesa|no quiero|terminar|cortá|corta|chau|adiós)\b/i.test(userText)) {
      ws.send(JSON.stringify({ type: 'text', token: 'Entendido. Gracias por tu tiempo. Hasta luego.', last: true, interruptible: false }));
      setTimeout(() => {
        if (ws.readyState === 1) ws.send(JSON.stringify({ type: 'end', handoffData: JSON.stringify({ reason: 'recipient-ended-call' }) }));
      }, 2500);
      call.status = 'completed';
      call.updatedAt = Date.now();
      return;
    }
    if (responding) return;
    responding = true;
    history.push({ role: 'user', content: userText });

    try {
      requireConfig(['OPENAI_API_KEY']);
      if (!openai) throw new Error('OPENAI_NOT_CONFIGURED');
      const stream = await openai.responses.create({
        model,
        instructions: call.instructions,
        input: history,
        stream: true,
        max_output_tokens: 250,
      });
      let answer = '';
      let pendingToken = '';
      for await (const chunk of stream) {
        if (chunk.type === 'response.output_text.delta') {
          answer += chunk.delta;
          if (pendingToken) ws.send(JSON.stringify({ type: 'text', token: pendingToken, last: false, interruptible: true }));
          pendingToken = chunk.delta;
        }
      }
      if (pendingToken) ws.send(JSON.stringify({ type: 'text', token: pendingToken, last: true, interruptible: true }));
      history.push({ role: 'assistant', content: answer });
      if (history.length > 24) history.splice(0, history.length - 24);
      call.transcript.push({ user: userText, assistant: answer, at: Date.now() });
      call.updatedAt = Date.now();
      saveCallProgress(call.reference, call).catch(() => {});
    } catch (error) {
      ws.send(JSON.stringify({ type: 'text', token: 'Disculpá, tuve un problema técnico. La llamada finalizará.', last: true }));
      call.error = error?.status === 401 ? 'OpenAI rechazó la credencial.' : error?.status === 429 ? 'OpenAI no tiene cuota disponible.' : 'No se pudo generar la respuesta.';
      call.updatedAt = Date.now();
      console.error('Error de IA durante llamada:', error?.status || error?.code || 'unknown');
    } finally {
      responding = false;
    }
  });
});

const heartbeat = setInterval(() => {
  for (const ws of wss.clients) {
    if (ws.isAlive === false) { ws.terminate(); continue; }
    ws.isAlive = false;
    ws.ping();
  }
}, 30000);
wss.on('close', () => clearInterval(heartbeat));

setInterval(() => {
  const cutoff = Date.now() - 24 * 60 * 60 * 1000;
  for (const [reference, call] of calls) if (call.createdAt < cutoff) calls.delete(reference);
  for (const [ip, times] of attemptsByIp) {
    const recent = times.filter((time) => time >= cutoff);
    if (recent.length) attemptsByIp.set(ip, recent); else attemptsByIp.delete(ip);
  }
}, 60 * 60 * 1000).unref();

initializeDatabase()
  .then(() => server.listen(port, () => console.log(`Llamador IA disponible en http://localhost:${port}`)))
  .catch((error) => { console.error('No se pudo inicializar PostgreSQL:', error.message); process.exit(1); });
