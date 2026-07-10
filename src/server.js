import 'dotenv/config';
import crypto from 'node:crypto';
import http from 'node:http';
import path from 'node:path';
import { fileURLToPath } from 'node:url';
import express from 'express';
import OpenAI from 'openai';
import twilio from 'twilio';
import { WebSocketServer } from 'ws';

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const app = express();
const server = http.createServer(app);
const wss = new WebSocketServer({ noServer: true });
const port = Number(process.env.PORT || 3000);
const publicUrl = (process.env.PUBLIC_URL || '').replace(/\/$/, '');
const model = process.env.OPENAI_MODEL || 'gpt-5.4-mini';
const maxCallSeconds = Math.min(Number(process.env.MAX_CALL_SECONDS || 300), 900);

app.use(express.json());
app.use(express.urlencoded({ extended: false }));
app.use(express.static(path.join(__dirname, '..', 'public')));

const calls = new Map();

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

app.get('/api/health', (_req, res) => {
  res.json({
    ok: true,
    mode: process.env.TWILIO_ACCOUNT_SID ? 'configured' : 'simulation-only',
    adminConfigured: String(process.env.CALL_ADMIN_TOKEN || '').trim().length >= 16,
  });
});

app.post('/api/calls', async (req, res) => {
  const to = String(req.body.to || '').replace(/[\s()-]/g, '');
  const fixedMessage = String(req.body.fixedMessage || '').trim();
  const instructions = String(req.body.instructions || '').trim();
  const dryRun = req.body.dryRun !== false;

  if (!argentinaNumber(to)) return res.status(400).json({ error: 'Usá formato internacional argentino: +54 seguido del número.' });
  if (!fixedMessage || fixedMessage.length > 1000) return res.status(400).json({ error: 'El mensaje debe tener entre 1 y 1000 caracteres.' });
  if (!instructions || instructions.length > 4000) return res.status(400).json({ error: 'Las instrucciones deben tener entre 1 y 4000 caracteres.' });
  if (!dryRun && !validAdminToken(req.body.adminToken)) {
    return res.status(403).json({ error: 'Clave administrativa incorrecta o no configurada.' });
  }

  const reference = crypto.randomUUID();
  calls.set(reference, { to, fixedMessage, instructions, createdAt: Date.now(), status: dryRun ? 'simulated' : 'queued' });
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
    res.json({ ok: true, dryRun: false, reference, sid: call.sid });
  } catch (error) {
    calls.get(reference).status = 'failed';
    res.status(500).json({ error: error.message });
  }
});

app.get('/api/calls/:reference', (req, res) => {
  const call = calls.get(req.params.reference);
  if (!call) return res.status(404).json({ error: 'Llamada no encontrada.' });
  res.json({ ...call, instructions: undefined });
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
    language: 'es-AR',
    interruptible: 'speech',
  });
  relay.parameter({ name: 'reference', value: String(req.query.reference) });
  res.type('text/xml').send(response.toString());
});

app.post('/twilio/status', (req, res) => {
  const call = calls.get(String(req.query.reference || ''));
  if (call) call.status = req.body.CallStatus || call.status;
  res.sendStatus(204);
});

server.on('upgrade', (request, socket, head) => {
  if (new URL(request.url, 'http://localhost').pathname !== '/conversation') return socket.destroy();
  wss.handleUpgrade(request, socket, head, (ws) => wss.emit('connection', ws, request));
});

wss.on('connection', (ws) => {
  let call;
  const history = [];

  ws.on('message', async (raw) => {
    let event;
    try { event = JSON.parse(raw.toString()); } catch { return; }
    if (event.type === 'setup') {
      call = calls.get(event.customParameters?.reference);
      if (!call) return ws.close(1008, 'Referencia desconocida');
      call.status = 'in-progress';
      return;
    }
    if (event.type !== 'prompt' || !event.last || !call) return;

    const userText = String(event.voicePrompt || '').trim();
    if (!userText) return;
    history.push({ role: 'user', content: userText });

    try {
      requireConfig(['OPENAI_API_KEY']);
      const openai = new OpenAI({ apiKey: process.env.OPENAI_API_KEY });
      const stream = await openai.responses.create({
        model,
        instructions: call.instructions,
        input: history,
        stream: true,
        max_output_tokens: 250,
      });
      let answer = '';
      for await (const chunk of stream) {
        if (chunk.type === 'response.output_text.delta') {
          answer += chunk.delta;
          ws.send(JSON.stringify({ type: 'text', token: chunk.delta, last: false, interruptible: true }));
        }
      }
      ws.send(JSON.stringify({ type: 'text', token: '', last: true, interruptible: true }));
      history.push({ role: 'assistant', content: answer });
      call.transcript = [...(call.transcript || []), { user: userText, assistant: answer }];
    } catch (error) {
      ws.send(JSON.stringify({ type: 'text', token: 'Disculpá, tuve un problema técnico. La llamada finalizará.', last: true }));
      call.error = error.message;
    }
  });
});

server.listen(port, () => console.log(`Llamador IA disponible en http://localhost:${port}`));
