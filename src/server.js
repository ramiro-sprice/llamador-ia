import 'dotenv/config';
import crypto from 'node:crypto';
import http from 'node:http';
import path from 'node:path';
import { fileURLToPath } from 'node:url';
import express from 'express';
import OpenAI from 'openai';
import twilio from 'twilio';
import multer from 'multer';
import { WebSocketServer } from 'ws';
import { createContact, databaseConfigured, deleteContacts, getAutomationSettings, getContact, importContacts, initializeDatabase, listContacts, saveAutomationSettings, saveCallProgress, saveCallStart, updateContact } from './database.js';
import { calendarConfigured, createCalendarEvent, schedulingContext } from './calendar.js';
import { parseContactFile } from './importer.js';

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
const upload = multer({ storage: multer.memoryStorage(), limits: { fileSize: 2 * 1024 * 1024, files: 1 } });
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
    appointmentCreated: Boolean(call.appointment?.eventId),
  };
}

async function detectConfirmedAppointment(call, history) {
  if (!openai || call.appointment?.eventId) return null;
  const conversation = history.map((item) => `${item.role === 'user' ? 'CONTACTO' : 'ASISTENTE'}: ${item.content}`).join('\n');
  if (!/llamad|agenda|horario|mañana|tarde|lunes|martes|miércoles|jueves|viernes/i.test(conversation)) return null;
  const extraction = await openai.responses.create({
    model,
    instructions: `Extraé una cita solamente si el contacto aceptó explícitamente una segunda llamada, quedaron definidos día y franja, y fueron informados el nombre de la persona y el nombre de la empresa o emprendimiento. ${schedulingContext()} Devolvé exclusivamente JSON válido, sin markdown: {"confirmed":boolean,"start":"ISO 8601 con -03:00","end":"ISO 8601 con -03:00","personName":string,"companyName":string,"role":string,"serviceInterest":string,"mainNeed":string,"website":string,"instagram":string,"facebook":string,"summary":string}. Para mañana usá 10:00; para tarde usá 15:00; duración 30 minutos. Si falta nombre, empresa, día, franja o confirmación inequívoca, confirmed debe ser false y start/end vacíos. En website/instagram/facebook copiá únicamente direcciones o usuarios explícitos; nunca escribas “sí”.`,
    input: conversation,
    max_output_tokens: 500,
  });
  try {
    const parsed = JSON.parse(extraction.output_text.replace(/^```json\s*|\s*```$/g, '').trim());
    return parsed.confirmed && parsed.start && parsed.end && parsed.personName && parsed.companyName ? parsed : null;
  } catch { return null; }
}

async function ensureAppointment(call, history) {
  if (!call || call.appointment?.eventId) return call?.appointment || null;
  if (call.appointmentPromise) return call.appointmentPromise;
  call.appointmentPromise = (async () => {
    const appointment = await detectConfirmedAppointment(call, history);
    if (!appointment) return null;
    const calendar = await createCalendarEvent({
      ...appointment,
      phone: call.to,
      firstCallAt: new Date(call.createdAt).toLocaleString('es-AR', { timeZone: 'America/Argentina/Buenos_Aires', hour12: false, year: 'numeric', month: '2-digit', day: '2-digit', hour: '2-digit', minute: '2-digit' }),
    });
    if (!calendar.ok) {
      call.error = `No se pudo crear la cita: ${calendar.error}`;
      return null;
    }
    call.appointment = { ...appointment, eventId: calendar.eventId };
    if (call.contactId) await updateContact(call.contactId, {
      personName: appointment.personName,
      companyName: appointment.companyName,
      role: appointment.role,
      website: appointment.website,
      instagram: appointment.instagram,
      facebook: appointment.facebook,
      nextCallAt: appointment.start,
      preferredPeriod: new Date(appointment.start).getHours() < 12 ? 'mañana' : 'tarde',
      status: 'scheduled',
      action: 'SEGUIMIENTO',
    });
    await saveCallProgress(call.reference, call);
    return call.appointment;
  })().finally(() => { call.appointmentPromise = null; });
  return call.appointmentPromise;
}

app.get('/api/health', (_req, res) => {
  res.json({
    ok: true,
    mode: process.env.TWILIO_ACCOUNT_SID ? 'configured' : 'simulation-only',
    adminConfigured: String(process.env.CALL_ADMIN_TOKEN || '').trim().length >= 16,
    databaseConfigured: databaseConfigured(),
    calendarConfigured: calendarConfigured(),
  });
});

app.get('/api/contacts', async (req, res) => {
  if (!validAdminToken(adminTokenFrom(req))) return res.status(403).json({ error: 'No autorizado.' });
  if (!databaseConfigured()) return res.status(503).json({ error: 'La base de datos todavía no está configurada.' });
  try { res.json({ contacts: await listContacts() }); }
  catch { res.status(500).json({ error: 'No se pudieron cargar los contactos.' }); }
});

app.get('/api/automation/settings', async (req, res) => {
  if (!validAdminToken(adminTokenFrom(req))) return res.status(403).json({ error: 'No autorizado.' });
  if (!databaseConfigured()) return res.status(503).json({ error: 'La base de datos todavía no está configurada.' });
  try { res.json({ settings: await getAutomationSettings() }); }
  catch { res.status(500).json({ error: 'No se pudo cargar la configuración.' }); }
});

app.put('/api/automation/settings', async (req, res) => {
  if (!validAdminToken(adminTokenFrom(req))) return res.status(403).json({ error: 'No autorizado.' });
  if (!databaseConfigured()) return res.status(503).json({ error: 'La base de datos todavía no está configurada.' });
  const time = (value) => /^([01]\d|2[0-3]):[0-5]\d$/.test(String(value || ''));
  const number = (value, min, max) => Number.isInteger(Number(value)) && Number(value) >= min && Number(value) <= max;
  const settings = {
    weekdays: Array.isArray(req.body.weekdays) ? [...new Set(req.body.weekdays.map(Number))].filter((day) => day >= 1 && day <= 5) : [],
    morningStart: req.body.morningStart, morningEnd: req.body.morningEnd,
    afternoonStart: req.body.afternoonStart, afternoonEnd: req.body.afternoonEnd,
    maxPerTenMinutes: Number(req.body.maxPerTenMinutes), concurrency: Number(req.body.concurrency), dailyMax: Number(req.body.dailyMax),
    delaySeconds: Number(req.body.delaySeconds), maxAttempts: Number(req.body.maxAttempts), retryHours: Number(req.body.retryHours),
  };
  if (!settings.weekdays.length) return res.status(400).json({ error: 'Seleccioná al menos un día hábil.' });
  if (![settings.morningStart,settings.morningEnd,settings.afternoonStart,settings.afternoonEnd].every(time) || settings.morningStart >= settings.morningEnd || settings.afternoonStart >= settings.afternoonEnd) return res.status(400).json({ error: 'Revisá los horarios configurados.' });
  if (!number(settings.maxPerTenMinutes,1,50) || !number(settings.concurrency,1,10) || !number(settings.dailyMax,1,1000) || !number(settings.delaySeconds,0,3600) || !number(settings.maxAttempts,1,20) || !number(settings.retryHours,1,720)) return res.status(400).json({ error: 'Uno de los límites está fuera del rango permitido.' });
  try { res.json({ settings: await saveAutomationSettings(settings) }); }
  catch { res.status(500).json({ error: 'No se pudo guardar la configuración.' }); }
});

app.post('/api/calendar/test', async (req, res) => {
  if (!validAdminToken(adminTokenFrom(req))) return res.status(403).json({ error: 'No autorizado.' });
  if (!calendarConfigured()) return res.status(503).json({ error: 'El calendario no está configurado.' });
  try {
    const result = await createCalendarEvent({
      start: '2020-01-02T10:00:00-03:00',
      end: '2020-01-02T10:30:00-03:00',
      personName: 'Prueba de conexión',
      companyName: 'Sprice',
    });
    if (result.error === 'El horario debe ser futuro') return res.json({ ok: true, message: 'Google Calendar está conectado y autenticado.' });
    res.status(502).json({ error: result.error || 'Apps Script respondió de forma inesperada.' });
  } catch { res.status(502).json({ error: 'No se pudo conectar con Google Apps Script.' }); }
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

app.post('/api/contacts/import', upload.single('file'), async (req, res) => {
  if (!validAdminToken(adminTokenFrom(req))) return res.status(403).json({ error: 'No autorizado.' });
  if (!databaseConfigured()) return res.status(503).json({ error: 'La base de datos todavía no está configurada.' });
  if (!req.file) return res.status(400).json({ error: 'Seleccioná un archivo .xlsx o .csv.' });
  try {
    const parsed = await parseContactFile(req.file);
    const imported = await importContacts(parsed.contacts);
    res.json({ created: imported.created, updated: imported.updated, rejected: [...parsed.rejected, ...imported.rejected] });
  } catch (error) { res.status(400).json({ error: error.message }); }
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

app.post('/api/contacts/delete', async (req, res) => {
  if (!validAdminToken(adminTokenFrom(req))) return res.status(403).json({ error: 'No autorizado.' });
  if (!databaseConfigured()) return res.status(503).json({ error: 'La base de datos todavía no está configurada.' });
  const ids = Array.isArray(req.body.ids) ? req.body.ids.filter((id) => /^[0-9a-f-]{36}$/i.test(id)).slice(0, 100) : [];
  if (!ids.length) return res.status(400).json({ error: 'No seleccionaste contactos válidos.' });
  try { res.json({ deleted: await deleteContacts(ids) }); }
  catch { res.status(500).json({ error: 'No se pudieron eliminar los contactos.' }); }
});

app.post('/api/calls', async (req, res) => {
  const to = String(req.body.to || '').replace(/[\s()-]/g, '');
  const fixedMessage = String(req.body.fixedMessage || '').trim();
  const instructions = String(req.body.instructions || '').trim();
  const contactId = String(req.body.contactId || '').trim() || null;
  const dryRun = req.body.dryRun !== false;

  if (!argentinaNumber(to)) return res.status(400).json({ error: 'Usá formato internacional argentino: +54 seguido del número.' });
  if (!fixedMessage || fixedMessage.length > 1000) return res.status(400).json({ error: 'El mensaje debe tener entre 1 y 1000 caracteres.' });
  if (!instructions || instructions.length > 8000) return res.status(400).json({ error: 'Las instrucciones deben tener entre 1 y 8000 caracteres.' });
  if (!dryRun && !validAdminToken(req.body.adminToken)) {
    return res.status(403).json({ error: 'Clave administrativa incorrecta o no configurada.' });
  }
  if (!dryRun) {
    const limitError = rateLimit(req, to);
    if (limitError) return res.status(429).json({ error: limitError });
  }

  let contact = null;
  if (contactId && databaseConfigured()) {
    try { contact = await getContact(contactId); }
    catch { return res.status(400).json({ error: 'No se pudo cargar la ficha del contacto.' }); }
  }
  const contactContext = contact ? `\n\nFICHA INTERNA DEL CONTACTO (usala como contexto; no leas esta ficha literalmente):\nNombre: ${contact.person_name || 'No informado'}\nEmpresa: ${contact.company_name || 'No informada'}\nKeywords: ${contact.keywords || 'No informadas'}\nWeb: ${contact.website || 'No informada'}\nNota manual: ${contact.notes || 'Sin nota manual'}\nHistorial de llamadas: ${contact.notes_history || 'Sin llamadas anteriores'}\nIntentos anteriores: ${contact.attempts || 0}.\nSi un dato figura como no informado, no lo inventes ni lo menciones. Usá las keywords de manera natural para comprender la actividad del contacto, nunca las recites como una lista.` : '';
  const values = { nombre: contact?.person_name, empresa: contact?.company_name, telefono: to, web: contact?.website, keywords: contact?.keywords };
  const personalizedMessage = fixedMessage.replace(/\{\{(nombre|empresa|telefono|web|keywords)\}\}/gi, (_match, key) => values[key.toLowerCase()] || '').replace(/\s{2,}/g, ' ').trim();
  const reference = crypto.randomUUID();
  calls.set(reference, { reference, to, contactId, fixedMessage: personalizedMessage, instructions: `${instructions}${contactContext}`, createdAt: Date.now(), updatedAt: Date.now(), status: dryRun ? 'simulated' : 'queued', transcript: [] });
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
    if (call.status === 'completed') {
      const finalHistory = (call.transcript || []).flatMap((turn) => [
        { role: 'user', content: turn.user || '' },
        { role: 'assistant', content: turn.assistant || '' },
      ]).filter((item) => item.content);
      ensureAppointment(call, finalHistory).catch((error) => {
        call.error = 'No se pudo verificar la segunda llamada al finalizar.';
        console.error('Error finalizando cita:', error?.message || error);
      });
    }
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
        instructions: `${call.instructions}\n\n${schedulingContext()}\nREGLAS OBLIGATORIAS PARA AGENDAR: antes de proponer o confirmar una segunda llamada, obtené el nombre de la persona y el nombre de la empresa o emprendimiento. Preguntá el cargo de forma natural; si prefiere no informarlo, podés continuar. Si dice que tiene web, Instagram o Facebook, pedí la dirección o usuario concreto, pero aceptá que prefiera no darlo. Confirmá en voz alta nombre, empresa, día y franja. No afirmes que una cita quedó agendada: cuando acepte, decí que vas a registrarla y que recibirá confirmación en esta misma llamada.`,
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
      const appointment = await ensureAppointment(call, history);
      if (appointment) {
          ws.send(JSON.stringify({ type: 'text', token: 'Perfecto. La segunda llamada quedó agendada y confirmada.', last: true, interruptible: true }));
      }
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

app.use((error, _req, res, _next) => {
  if (error instanceof multer.MulterError) {
    const message = error.code === 'LIMIT_FILE_SIZE' ? 'El archivo supera el límite de 2 MB.' : 'No se pudo recibir el archivo.';
    return res.status(400).json({ error: message });
  }
  console.error('Error HTTP no controlado:', error?.message || 'unknown');
  res.status(500).json({ error: 'Ocurrió un error inesperado.' });
});

initializeDatabase()
  .then(() => server.listen(port, () => console.log(`Llamador IA disponible en http://localhost:${port}`)))
  .catch((error) => { console.error('No se pudo inicializar PostgreSQL:', error.message); process.exit(1); });
