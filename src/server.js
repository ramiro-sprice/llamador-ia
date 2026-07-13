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
import { automationStats, claimNextAutomationContact, createContact, databaseConfigured, deleteContacts, getAutomationSettings, getContact, importContacts, initializeDatabase, listContacts, recoverInterruptedContacts, releaseAutomationContact, saveAutomationSettings, saveCallProgress, saveCallRecording, saveCallStart, setAutomationState, updateContact } from './database.js';
import { automationAllowedNow, schedulingContext } from './calendar.js';
import { parseContactFile } from './importer.js';

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const app = express();
app.set('trust proxy', 1);
const server = http.createServer(app);
const wss = new WebSocketServer({ noServer: true });
const mediaWss = new WebSocketServer({ noServer: true });
const monitorWss = new WebSocketServer({ noServer: true });
const port = Number(process.env.PORT || 3000);
const publicUrl = (process.env.PUBLIC_URL || '').replace(/\/$/, '');
const model = process.env.OPENAI_MODEL || 'gpt-5.4-mini';
const defaultMaxCallSeconds = 8 * 60;
const openai = process.env.OPENAI_API_KEY ? new OpenAI({ apiKey: process.env.OPENAI_API_KEY }) : null;
const twilioElevenLabsVoiceId = String(process.env.TWILIO_ELEVENLABS_VOICE_ID || '').trim();
const twilioAccountSid = String(process.env.TWILIO_ACCOUNT_SID || '').trim();
const twilioAuthToken = String(process.env.TWILIO_AUTH_TOKEN || '').trim();
const twilioApiKeySid = String(process.env.TWILIO_API_KEY_SID || '').trim();
const twilioApiKeySecret = String(process.env.TWILIO_API_KEY_SECRET || '').trim();
const twilioPhoneNumber = String(process.env.TWILIO_PHONE_NUMBER || '').trim();
const RATE_WINDOW_MS = 10 * 60 * 1000;
const RATE_MAX_CALLS = 5;
const NUMBER_COOLDOWN_MS = 60 * 1000;

app.use(express.json());
app.use(express.urlencoded({ extended: false }));
app.use(express.static(path.join(__dirname, '..', 'public')));

const calls = new Map();
const monitorTickets = new Map();
const monitorBrowsers = new Map();
const upload = multer({ storage: multer.memoryStorage(), limits: { fileSize: 2 * 1024 * 1024, files: 1 } });
const attemptsByIp = new Map();
const lastCallByNumber = new Map();
let automationTickBusy = false;
let lastAutomationLaunchAt = 0;
let lastAutomationEvent = { type:'idle', message:'El motor todavía no realizó intentos en este inicio.', at:null, phone:null };

function twilioClient() {
  const useApiKey = /^SK[0-9a-f]{32}$/i.test(twilioApiKeySid) && twilioApiKeySecret.length >= 20;
  return useApiKey
    ? twilio(twilioApiKeySid, twilioApiKeySecret, { accountSid: twilioAccountSid })
    : twilio(twilioAccountSid, twilioAuthToken);
}

async function stopMonitorStream(call) {
  if (!call?.monitorStreamSid || !call.sid) return;
  const streamSid = call.monitorStreamSid;
  call.monitorStreamSid = null;
  await twilioClient().calls(call.sid).streams(streamSid).update({ status: 'stopped' }).catch(() => {});
}

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
  if (code === 'TWILIO_ACCOUNT_SID_FORMAT') return 'TWILIO_ACCOUNT_SID tiene un formato incorrecto: debe comenzar con AC.';
  if (code === 'TWILIO_AUTH_TOKEN_FORMAT') return 'TWILIO_AUTH_TOKEN está vacío o incompleto.';
  if (code === 20003 || /authentication error|invalid username|authenticate/i.test(String(error?.message || ''))) return 'Twilio rechazó las credenciales. Verificá que TWILIO_ACCOUNT_SID y TWILIO_AUTH_TOKEN pertenezcan a la misma cuenta activa.';
  if (error?.status === 401) return 'OpenAI rechazó la credencial configurada.';
  if (error?.status === 429) return 'OpenAI no tiene cuota disponible o alcanzó su límite.';
  if (code === 21215 || code === 21219) return 'Twilio no permite usar el número emisor configurado.';
  if (code === 21211) return 'El número destinatario no es válido para Twilio.';
  if (code === 21408) return 'Twilio no tiene habilitadas las llamadas hacia Argentina.';
  return 'El proveedor telefónico rechazó la llamada. Revisá los registros de Twilio.';
}

function greetingForVoice(text) {
  if (twilioElevenLabsVoiceId) return String(text || '').replace(/\[\[\]\]/g, '…\n\n');
  const escaped = String(text || '').replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;').replace(/"/g, '&quot;').replace(/'/g, '&apos;');
  return `<speak><prosody volume="x-loud">${escaped.replace(/\[\[\]\]/g, '<break time="1s"/>')}</prosody></speak>`;
}

function speechToken(text) {
  return String(text || '');
}

function endAfterSpeech(ws, call, text, reason = 'assistant-ended-call') {
  if (!call || call.endScheduled) return;
  call.endScheduled = true;
  const words = String(text || '').trim().split(/\s+/).filter(Boolean).length;
  const delay = Math.min(12000, Math.max(2500, Math.ceil(words / 2.2 * 1000) + 1200));
  setTimeout(() => {
    if (ws.readyState === 1) ws.send(JSON.stringify({ type: 'end', handoffData: JSON.stringify({ reason }) }));
    call.status = 'completed';
    call.updatedAt = Date.now();
    saveCallProgress(call.reference, call).catch(() => {});
  }, delay);
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

async function startOutboundCall({ to, contactId, fixedMessage, instructions, automated = false, maxCallSeconds = defaultMaxCallSeconds }) {
  let contact = null;
  if (contactId && databaseConfigured()) contact = await getContact(contactId);
  const contactContext = contact ? `\n\nFICHA INTERNA DEL CONTACTO (usala como contexto; no leas esta ficha literalmente):\nNombre: ${contact.person_name || 'No informado'}\nEmpresa: ${contact.company_name || 'No informada'}\nKeywords: ${contact.keywords || 'No informadas'}\nWeb: ${contact.website || 'No informada'}\nNota manual: ${contact.notes || 'Sin nota manual'}\nHistorial de llamadas: ${contact.notes_history || 'Sin llamadas anteriores'}\nIntentos anteriores: ${contact.attempts || 0}.\nSi un dato figura como no informado, no lo inventes ni lo menciones. Usá las keywords de manera natural para comprender la actividad del contacto, nunca las recites como una lista.` : '';
  const values = { nombre: contact?.person_name, empresa: contact?.company_name, telefono: to, web: contact?.website, keywords: contact?.keywords || 'servicios como los de ustedes' };
  const personalizedMessage = fixedMessage.replace(/\{\{(nombre|empresa|telefono|web|keywords)\}\}/gi, (_match, key) => values[key.toLowerCase()] || '').trim();
  const reference = crypto.randomUUID();
  calls.set(reference, { reference, to, contactId, fixedMessage: personalizedMessage, instructions: `${instructions}${contactContext}`, createdAt: Date.now(), updatedAt: Date.now(), status: 'queued', transcript: [], automated, maxCallSeconds });
  requireConfig(['PUBLIC_URL', 'TWILIO_ACCOUNT_SID', 'TWILIO_PHONE_NUMBER', 'OPENAI_API_KEY']);
  if (!/^AC[0-9a-f]{32}$/i.test(twilioAccountSid)) throw Object.assign(new Error('TWILIO_ACCOUNT_SID_FORMAT'), { code:'TWILIO_ACCOUNT_SID_FORMAT' });
  const useApiKey = /^SK[0-9a-f]{32}$/i.test(twilioApiKeySid) && twilioApiKeySecret.length >= 20;
  if (!useApiKey && twilioAuthToken.length < 20) throw Object.assign(new Error('TWILIO_AUTH_TOKEN_FORMAT'), { code:'TWILIO_AUTH_TOKEN_FORMAT' });
  const client = twilioClient();
  const outbound = await client.calls.create({
    to,
    from: twilioPhoneNumber,
    url: `${publicUrl}/twilio/voice?reference=${encodeURIComponent(reference)}`,
    statusCallback: `${publicUrl}/twilio/status?reference=${encodeURIComponent(reference)}`,
    statusCallbackEvent: ['initiated','ringing','answered','completed'],
    record: true,
    recordingChannels: 'dual',
    recordingStatusCallback: `${publicUrl}/twilio/recording?reference=${encodeURIComponent(reference)}`,
    recordingStatusCallbackEvent: ['completed','absent'],
    timeout: 30,
    timeLimit: maxCallSeconds,
  });
  calls.get(reference).sid = outbound.sid;
  await saveCallStart(reference, contactId, outbound.sid, 'queued');
  return { reference, sid: outbound.sid };
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
    appointmentCreated: Boolean(call.appointment?.registered),
  };
}

async function detectConfirmedAppointment(call, history) {
  if (!openai || call.appointment?.registered) return null;
  const conversation = history.map((item) => `${item.role === 'user' ? 'CONTACTO' : 'ASISTENTE'}: ${item.content}`).join('\n');
  if (!/llamad|agenda|horario|mañana|tarde|lunes|martes|miércoles|jueves|viernes/i.test(conversation)) return null;
  const extraction = await openai.responses.create({
    model,
    instructions: `Extraé un seguimiento solamente si aceptaron explícitamente otra llamada y quedaron definidos día y franja. También puede aceptarlo quien atendió cuando informa que la persona responsable no está y dice cuándo encontrarla. ${schedulingContext()} Devolvé exclusivamente JSON válido, sin markdown: {"confirmed":boolean,"start":"ISO 8601 con -03:00","end":"ISO 8601 con -03:00","personName":string,"companyName":string,"role":string,"serviceInterest":string,"mainNeed":string,"website":string,"instagram":string,"facebook":string,"summary":string,"noTimeCallback":boolean,"decisionMakerAbsent":boolean,"gatekeeperName":string,"samePhone":boolean,"alternatePhone":string}. Para mañana usá 10:00; para tarde usá 15:00; duración 30 minutos. personName debe ser la persona a quien se llamará después. Si la persona responsable no estaba, decisionMakerAbsent debe ser true, gatekeeperName debe contener quién atendió si lo dijo, samePhone debe indicar si se la encuentra en el número actual y alternatePhone debe contener el otro número solamente si fue informado. El nombre de la persona a llamar siempre es obligatorio. La empresa es obligatoria salvo falta de tiempo o responsable ausente. Si falta nombre, día, franja o aceptación inequívoca, confirmed debe ser false. El summary debe comenzar con “Volver a llamar” e incluir fecha, franja, responsable y quién atendió cuando se conozca. En website/instagram/facebook copiá únicamente direcciones o usuarios explícitos; nunca escribas “sí”.`,
    input: conversation,
    max_output_tokens: 500,
  });
  try {
    const parsed = JSON.parse(extraction.output_text.replace(/^```json\s*|\s*```$/g, '').trim());
    return parsed.confirmed && parsed.start && parsed.end && parsed.personName && (parsed.companyName || parsed.noTimeCallback || parsed.decisionMakerAbsent) ? parsed : null;
  } catch { return null; }
}

async function ensureAppointment(call, history) {
  if (!call || call.appointment?.registered) return call?.appointment || null;
  if (call.appointmentPromise) return call.appointmentPromise;
  call.appointmentPromise = (async () => {
    const appointment = await detectConfirmedAppointment(call, history);
    if (!appointment) return null;
    call.appointment = { ...appointment, registered: true, calendarSuspended: true };
    const preferredPeriod = new Date(appointment.start).getHours() < 12 ? 'mañana' : 'tarde';
    const alternatePhone = String(appointment.alternatePhone || '').replace(/[\s()-]/g, '');
    const useAlternatePhone = appointment.decisionMakerAbsent && !appointment.samePhone && argentinaNumber(alternatePhone) && alternatePhone !== call.to;
    if (useAlternatePhone) {
      const newId = crypto.randomUUID();
      try {
        await createContact({
          id: newId,
          phone: alternatePhone,
          personName: appointment.personName,
          companyName: appointment.companyName,
          role: appointment.role,
          notes: appointment.summary,
          source: 'referido-en-llamada',
          referredBy: appointment.gatekeeperName || call.to,
          consentStatus: 'unknown',
          status: 'callback',
        });
        await updateContact(newId, { nextCallAt: appointment.start, preferredPeriod, status: 'callback', action: 'SEGUIMIENTO' });
      } catch (error) {
        console.error('No se pudo crear el contacto referido:', error?.message || error);
      }
      if (call.contactId) await updateContact(call.contactId, { status: 'callback', action: 'REFERIDO' });
    } else if (call.contactId) {
      await updateContact(call.contactId, {
        personName: appointment.personName,
        ...(appointment.companyName ? { companyName: appointment.companyName } : {}),
        role: appointment.role,
        website: appointment.website,
        instagram: appointment.instagram,
        facebook: appointment.facebook,
        nextCallAt: appointment.start,
        preferredPeriod,
        status: appointment.decisionMakerAbsent ? 'callback' : 'scheduled',
        action: 'SEGUIMIENTO',
      });
    }
    await saveCallProgress(call.reference, call);
    return call.appointment;
  })().finally(() => { call.appointmentPromise = null; });
  return call.appointmentPromise;
}

app.get('/api/health', (_req, res) => {
  const apiKeyReady = /^SK[0-9a-f]{32}$/i.test(twilioApiKeySid) && twilioApiKeySecret.length >= 20;
  res.json({
    ok: true,
    mode: twilioAccountSid ? 'configured' : 'simulation-only',
    twilioAccountSidFormat: /^AC[0-9a-f]{32}$/i.test(twilioAccountSid),
    twilioAuthTokenPresent: twilioAuthToken.length >= 20,
    twilioAuthenticationMethod: apiKeyReady ? 'api-key' : 'auth-token',
    twilioAccountSidEnding: twilioAccountSid.slice(-4),
    twilioApiKeySidFormat: /^SK[0-9a-f]{32}$/i.test(twilioApiKeySid),
    twilioApiKeySidEnding: twilioApiKeySid.slice(-4),
    twilioApiKeySecretPresent: twilioApiKeySecret.length >= 20,
    twilioPhoneFormat: /^\+\d{10,15}$/.test(twilioPhoneNumber),
    adminConfigured: String(process.env.CALL_ADMIN_TOKEN || '').trim().length >= 16,
    databaseConfigured: databaseConfigured(),
    calendarConfigured: false,
    calendarSuspended: true,
  });
});

app.get('/api/contacts', async (req, res) => {
  if (!validAdminToken(adminTokenFrom(req))) return res.status(403).json({ error: 'No autorizado.' });
  if (!databaseConfigured()) return res.status(503).json({ error: 'La base de datos todavía no está configurada.' });
  try {
    const contacts = await listContacts();
    const activeCalls = [...calls.values()].filter((call) => ['answered','in-progress'].includes(call.status));
    const activeByContact = new Map(activeCalls.filter((call) => call.contactId).map((call) => [String(call.contactId), call.reference]));
    const activeByPhone = new Map(activeCalls.map((call) => [String(call.to || '').replace(/[\s()-]/g, ''), call.reference]));
    res.json({ contacts: contacts.map((contact) => ({
      ...contact,
      active_reference: activeByContact.get(String(contact.id)) || activeByPhone.get(String(contact.phone || '').replace(/[\s()-]/g, '')) || null,
    })) });
  }
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
    weekdays: Array.isArray(req.body.weekdays) ? [...new Set(req.body.weekdays.map(Number))].filter((day) => day >= 1 && day <= 7) : [],
    morningStart: req.body.morningStart, morningEnd: req.body.morningEnd,
    afternoonStart: req.body.afternoonStart, afternoonEnd: req.body.afternoonEnd,
    maxPerTenMinutes: Number(req.body.maxPerTenMinutes), concurrency: Number(req.body.concurrency), dailyMax: Number(req.body.dailyMax),
    delaySeconds: Number(req.body.delaySeconds), maxAttempts: Number(req.body.maxAttempts), retryHours: Number(req.body.retryHours), maxCallMinutes: Number(req.body.maxCallMinutes),
  };
  if (!settings.weekdays.length) return res.status(400).json({ error: 'Seleccioná al menos un día hábil.' });
  if (![settings.morningStart,settings.morningEnd,settings.afternoonStart,settings.afternoonEnd].every(time) || settings.morningStart >= settings.morningEnd || settings.afternoonStart >= settings.afternoonEnd) return res.status(400).json({ error: 'Revisá los horarios configurados.' });
  if (!number(settings.maxPerTenMinutes,1,50) || !number(settings.concurrency,1,10) || !number(settings.dailyMax,1,1000) || !number(settings.delaySeconds,0,3600) || !number(settings.maxAttempts,1,20) || !number(settings.retryHours,1,720) || !number(settings.maxCallMinutes,3,30)) return res.status(400).json({ error: 'Uno de los límites está fuera del rango permitido.' });
  try { res.json({ settings: await saveAutomationSettings(settings) }); }
  catch { res.status(500).json({ error: 'No se pudo guardar la configuración.' }); }
});

app.get('/api/automation/status', async (req, res) => {
  if (!validAdminToken(adminTokenFrom(req))) return res.status(403).json({ error: 'No autorizado.' });
  try {
    const settings = await getAutomationSettings();
    const stats = await automationStats();
    const withinSchedule = automationAllowedNow(settings);
    let blockedReason = null;
    if (settings.status === 'running') {
      if (!settings.fixed_message || !settings.instructions) blockedReason = 'Falta guardar el mensaje inicial o el guion.';
      else if (!withinSchedule) blockedReason = 'Esperando el próximo día y horario habilitado.';
      else if (stats.eligible < 1) blockedReason = 'No hay contactos pendientes elegibles.';
      else if (stats.active_contacts >= settings.concurrency) blockedReason = 'Esperando que termine una llamada activa.';
      else if (stats.calls_last_ten_minutes >= settings.max_per_ten_minutes) blockedReason = 'Se alcanzó el máximo configurado para los últimos 10 minutos.';
      else if (stats.calls_today >= settings.daily_max) blockedReason = 'Se alcanzó el máximo diario configurado.';
      else if (Date.now() - lastAutomationLaunchAt < settings.delay_seconds * 1000) blockedReason = 'Esperando el intervalo configurado entre llamadas.';
    }
    res.json({ settings, stats, withinSchedule, blockedReason, lastAutomationEvent, activeCalls: [...calls.values()].filter((call) => call.automated && !['completed','failed','busy','no-answer','canceled'].includes(call.status)).length });
  } catch { res.status(500).json({ error: 'No se pudo consultar la automatización.' }); }
});

app.post('/api/automation/start', async (req, res) => {
  if (!validAdminToken(adminTokenFrom(req))) return res.status(403).json({ error: 'No autorizado.' });
  const fixedMessage = String(req.body.fixedMessage || '').trim();
  const instructions = String(req.body.instructions || '').trim();
  if (!fixedMessage || fixedMessage.length > 1000 || !instructions || instructions.length > 12000) return res.status(400).json({ error: 'El mensaje inicial o el guion no son válidos.' });
  try {
    const settings = await setAutomationState('running', { fixedMessage, instructions });
    res.json({ settings, stats: await automationStats() });
    setImmediate(() => runAutomationTick());
  }
  catch { res.status(500).json({ error: 'No se pudo iniciar la automatización.' }); }
});

app.post('/api/automation/pause', async (req, res) => {
  if (!validAdminToken(adminTokenFrom(req))) return res.status(403).json({ error: 'No autorizado.' });
  try { res.json({ settings: await setAutomationState('paused') }); }
  catch { res.status(500).json({ error: 'No se pudo pausar la automatización.' }); }
});

app.post('/api/automation/stop', async (req, res) => {
  if (!validAdminToken(adminTokenFrom(req))) return res.status(403).json({ error: 'No autorizado.' });
  try { res.json({ settings: await setAutomationState('stopped') }); }
  catch { res.status(500).json({ error: 'No se pudo detener la automatización.' }); }
});

app.post('/api/calendar/test', async (req, res) => {
  if (!validAdminToken(adminTokenFrom(req))) return res.status(403).json({ error: 'No autorizado.' });
  res.status(503).json({ error: 'Google Calendar está suspendido temporalmente.' });
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
  if (!instructions || instructions.length > 12000) return res.status(400).json({ error: 'Las instrucciones deben tener entre 1 y 12000 caracteres.' });
  if (!dryRun && !validAdminToken(req.body.adminToken)) {
    return res.status(403).json({ error: 'Clave administrativa incorrecta o no configurada.' });
  }
  if (!dryRun) {
    const limitError = rateLimit(req, to);
    if (limitError) return res.status(429).json({ error: limitError });
  }

  if (dryRun) return res.json({ ok: true, dryRun: true, reference: crypto.randomUUID(), message: 'Simulación válida; no se realizó ninguna llamada.' });

  try {
    const started = await startOutboundCall({ to, contactId, fixedMessage, instructions });
    res.json({ ok: true, dryRun: false, ...started });
  } catch (error) {
    if (contactId) await releaseAutomationContact(contactId, 'failed').catch(() => {});
    res.status(502).json({ error: safeCallError(error) });
  }
});

app.get('/api/calls/:reference', (req, res) => {
  if (!validAdminToken(adminTokenFrom(req))) return res.status(403).json({ error: 'No autorizado.' });
  const call = calls.get(req.params.reference);
  if (!call) return res.status(404).json({ error: 'Llamada no encontrada.' });
  res.json(publicCall(req.params.reference, call));
});

app.post('/api/calls/:reference/monitor/start', async (req, res) => {
  if (!validAdminToken(adminTokenFrom(req))) return res.status(403).json({ error: 'No autorizado.' });
  const call = calls.get(req.params.reference);
  if (!call || !call.sid || !['answered','in-progress'].includes(call.status)) return res.status(409).json({ error: 'La llamada ya no está disponible para escuchar.' });
  try {
    if (!call.monitorSecret) call.monitorSecret = crypto.randomBytes(24).toString('hex');
    if (!call.monitorStreamSid) {
      const stream = await twilioClient().calls(call.sid).streams.create({
        url: `${publicUrl.replace(/^http/, 'ws')}/media/${call.reference}/${call.monitorSecret}`,
        track: 'both_tracks',
      });
      call.monitorStreamSid = stream.sid;
    }
    const ticket = crypto.randomBytes(24).toString('hex');
    monitorTickets.set(ticket, { reference: call.reference, expiresAt: Date.now() + 60000 });
    res.json({ ticket, websocketUrl: `${publicUrl.replace(/^http/, 'ws')}/monitor?ticket=${ticket}` });
  } catch (error) {
    console.error('No se pudo iniciar el monitoreo:', error?.code || error?.message || 'unknown');
    res.status(502).json({ error: 'Twilio no pudo abrir el audio en vivo.' });
  }
});

app.post('/api/calls/:reference/monitor/stop', async (req, res) => {
  if (!validAdminToken(adminTokenFrom(req))) return res.status(403).json({ error: 'No autorizado.' });
  const call = calls.get(req.params.reference);
  await stopMonitorStream(call);
  for (const browser of monitorBrowsers.get(req.params.reference) || []) browser.close(1000, 'Monitoreo detenido');
  res.json({ ok: true });
});

app.post('/api/calls/:reference/hangup', async (req, res) => {
  if (!validAdminToken(adminTokenFrom(req))) return res.status(403).json({ error: 'No autorizado.' });
  const call = calls.get(req.params.reference);
  if (!call?.sid || ['completed','failed','busy','no-answer','canceled'].includes(call.status)) return res.status(409).json({ error: 'La llamada ya finalizó.' });
  try {
    await twilioClient().calls(call.sid).update({ status: 'completed' });
    call.status = 'completed';
    call.updatedAt = Date.now();
    await stopMonitorStream(call);
    await saveCallProgress(call.reference, call);
    if (call.contactId) await updateContact(call.contactId, { status: 'callback', action: 'REINTENTAR' });
    res.json({ ok: true, status: call.status });
  } catch (error) {
    console.error('No se pudo cortar la llamada:', error?.code || error?.message || 'unknown');
    res.status(502).json({ error: 'Twilio no pudo finalizar la llamada.' });
  }
});

app.post('/twilio/voice', (req, res) => {
  const call = calls.get(String(req.query.reference || ''));
  if (!call) return res.status(404).type('text/xml').send('<Response><Hangup/></Response>');
  const response = new twilio.twiml.VoiceResponse();
  const connect = response.connect();
  const relay = connect.conversationRelay({
    url: `${publicUrl.replace(/^http/, 'ws')}/conversation`,
    welcomeGreeting: greetingForVoice(call.fixedMessage),
    welcomeGreetingInterruptible: 'none',
    ttsLanguage: twilioElevenLabsVoiceId ? 'en-US' : 'es-MX',
    ttsProvider: twilioElevenLabsVoiceId ? 'ElevenLabs' : 'Amazon',
    voice: twilioElevenLabsVoiceId || 'Mia-Neural',
    ...(twilioElevenLabsVoiceId ? { elevenlabsTextNormalization: 'on' } : {}),
    transcriptionLanguage: 'es-US',
    speechTimeout: 2500,
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
      stopMonitorStream(call).catch(() => {});
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

app.post('/twilio/recording', async (req, res) => {
  const reference = String(req.query.reference || '');
  const call = calls.get(reference);
  if (call) {
    call.recording = {
      sid: String(req.body.RecordingSid || ''),
      status: String(req.body.RecordingStatus || ''),
      duration: Number.parseInt(req.body.RecordingDuration || '', 10),
    };
  }
  await saveCallRecording(reference, {
    sid: String(req.body.RecordingSid || ''),
    status: String(req.body.RecordingStatus || ''),
    duration: Number.parseInt(req.body.RecordingDuration || '', 10),
  }).catch(() => {});
  res.sendStatus(204);
});

server.on('upgrade', (request, socket, head) => {
  const url = new URL(request.url, 'http://localhost');
  if (url.pathname === '/conversation') return wss.handleUpgrade(request, socket, head, (ws) => wss.emit('connection', ws, request));
  if (url.pathname.startsWith('/media/')) return mediaWss.handleUpgrade(request, socket, head, (ws) => mediaWss.emit('connection', ws, request));
  if (url.pathname === '/monitor') return monitorWss.handleUpgrade(request, socket, head, (ws) => monitorWss.emit('connection', ws, request));
  socket.destroy();
});

mediaWss.on('connection', (ws, request) => {
  const parts = new URL(request.url, 'http://localhost').pathname.split('/');
  const reference = parts[2];
  const secret = parts[3];
  const call = calls.get(reference);
  if (!call || !call.monitorSecret || secret !== call.monitorSecret) return ws.close(1008, 'No autorizado');
  ws.on('message', (raw) => {
    let event;
    try { event = JSON.parse(raw.toString()); } catch { return; }
    if (event.event !== 'media' || !event.media?.payload) return;
    const message = JSON.stringify({ type:'audio', track:event.media.track || 'unknown', payload:event.media.payload });
    for (const browser of monitorBrowsers.get(reference) || []) if (browser.readyState === 1) browser.send(message);
  });
  ws.on('close', () => {
    for (const browser of monitorBrowsers.get(reference) || []) if (browser.readyState === 1) browser.send(JSON.stringify({ type:'ended' }));
  });
});

monitorWss.on('connection', (ws, request) => {
  const ticket = new URL(request.url, 'http://localhost').searchParams.get('ticket');
  const access = monitorTickets.get(ticket);
  monitorTickets.delete(ticket);
  if (!access || access.expiresAt < Date.now() || !calls.has(access.reference)) return ws.close(1008, 'No autorizado');
  const browsers = monitorBrowsers.get(access.reference) || new Set();
  browsers.add(ws);
  monitorBrowsers.set(access.reference, browsers);
  ws.send(JSON.stringify({ type:'ready' }));
  ws.on('close', () => {
    browsers.delete(ws);
    if (!browsers.size) {
      monitorBrowsers.delete(access.reference);
      stopMonitorStream(calls.get(access.reference)).catch(() => {});
    }
  });
});

wss.on('connection', (ws) => {
  let call;
  const history = [];
  let responding = false;
  let wrapUpTimer;
  let finalFarewellTimer;
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
      history.push({ role: 'assistant', content: call.fixedMessage });
      const requestWrapUp = () => {
        if (!call || call.endScheduled || call.appointment?.registered || ws.readyState !== 1) return;
        if (responding) { wrapUpTimer = setTimeout(requestWrapUp, 3000); return; }
        const transition = 'Para respetar tu tiempo, creo que lo mejor es que un asesor con más experiencia te vuelva a llamar y lo conversen con tranquilidad. ¿Qué día y horario te resultan más cómodos?';
        call.wrapUpRequested = true;
        history.push({ role: 'assistant', content: transition });
        call.transcript.push({ user: '', assistant: transition, at: Date.now() });
        call.updatedAt = Date.now();
        ws.send(JSON.stringify({ type: 'text', token: speechToken(transition), last: true, interruptible: true, preemptible: false }));
        saveCallProgress(call.reference, call).catch(() => {});
      };
      const configuredMaximum = call.maxCallSeconds || defaultMaxCallSeconds;
      wrapUpTimer = setTimeout(requestWrapUp, Math.max(60, configuredMaximum - 120) * 1000);
      const requestFinalFarewell = () => {
        if (!call || call.endScheduled || ws.readyState !== 1) return;
        if (responding) { finalFarewellTimer = setTimeout(requestFinalFarewell, 2000); return; }
        const farewell = 'Muchas gracias por tu tiempo. Para no extenderme más, dejamos la conversación acá y continuamos en la próxima llamada. Que tengas un buen día.';
        ws.send(JSON.stringify({ type: 'text', token: speechToken(farewell), last: true, interruptible: false, preemptible: false }));
        endAfterSpeech(ws, call, farewell, 'maximum-duration-reached');
      };
      finalFarewellTimer = setTimeout(requestFinalFarewell, Math.max(90, configuredMaximum - 30) * 1000);
      return;
    }
    if (event.type !== 'prompt' || !event.last || !call) return;

    const userText = String(event.voicePrompt || '').trim();
    if (!userText) return;
    if (/\b(no me llamen|no me interesa|no quiero|terminar|cortá|corta|chau|adiós)\b/i.test(userText)) {
      const farewell = 'Entendido. Gracias por tu tiempo. Hasta luego.';
      ws.send(JSON.stringify({ type: 'text', token: speechToken(farewell), last: true, interruptible: false }));
      endAfterSpeech(ws, call, farewell, 'recipient-ended-call');
      call.status = 'completed';
      call.updatedAt = Date.now();
      if (call.contactId && /\b(no me llamen|no me interesa|no quiero)\b/i.test(userText)) updateContact(call.contactId, { status: 'not-interested', action: 'NO_LLAMAR' }).catch(() => {});
      saveCallProgress(call.reference, call).catch(() => {});
      return;
    }
    const hesitant = /\b(no s[eé]|puede ser|quiz[aá]s|capaz|tal vez|lo (?:voy a|tengo que) pensar|dejame pensarlo|d[eé]jame pensarlo|no estoy seguro|no estoy segura|mmm+|ehh+)\b/i.test(userText);
    if (hesitant) call.hesitationCount = (call.hesitationCount || 0) + 1;
    else call.hesitationCount = 0;
    if (call.hesitationCount >= 2) {
      const transition = 'Entiendo. Para no quitarte más tiempo, ¿preferís que un asesor te llame en otro momento?';
      history.push({ role: 'user', content: userText }, { role: 'assistant', content: transition });
      call.transcript.push({ user: userText, assistant: transition, at: Date.now() });
      call.updatedAt = Date.now();
      ws.send(JSON.stringify({ type: 'text', token: speechToken(transition), last: true, interruptible: true }));
      saveCallProgress(call.reference, call).catch(() => {});
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
        instructions: `${call.instructions}\n\n${schedulingContext()}\nCONTROL ESTRICTO DE RESPUESTA: El mensaje inicial ya fue pronunciado y figura en el historial. No pidas el nombre hasta detectar interés, necesitar identificar a quien decide o coordinar otra llamada. Cuando la persona diga su nombre, guardalo y continuá directamente: no agradezcas ni digas “mucho gusto”. Si acepta escuchar el mensaje inicial, preguntá directamente “¿Actualmente tienen una página web?” sin agregar explicaciones. El objetivo de esta primera llamada es únicamente detectar una necesidad, identificar a quien decide y conseguir permiso para que un asesor vuelva a llamar; no intentes resolver ni explicar todo. Si dice que no toma las decisiones, preguntá si la persona responsable está en ese momento. Si está, pedí: “¿Podrías comunicarme con esa persona, por favor?” y esperá la transferencia sin seguir vendiendo. Cuando atienda la persona responsable, no vuelvas a presentarte por tu nombre; retomá exactamente con: “Te llamo de Sprice. Nos dedicamos a crear y mejorar páginas web. No quiero venderte nada ahora; quería contarte brevemente cómo podrías mejorar tus ventas. ¿Tenés un minuto?”. Si informa que la persona responsable no está, dejá de presentar servicios. Preguntá, de a una por turno: nombre del responsable, día y franja para encontrarlo, y si se lo encuentra en el mismo número. Si brinda otro teléfono, confirmalo una vez. Respondé exclusivamente sobre páginas web, tiendas online, presencia en Google, publicidad digital o coordinación de una devolución de llamada de Sprice. No cambies de tema. No inventes ejemplos, estadísticas, diagnósticos, servicios, promociones ni información de la empresa. SÉ MUY BREVE: cada turno debe tener como máximo una afirmación corta y una sola pregunta, idealmente menos de 25 palabras y menos de 10 segundos al hablar. No hagas introducciones, listas, resúmenes ni explicaciones largas. Si te piden detalles, explicá un solo punto y esperá. Si no entendiste, pedí una aclaración breve; nunca rellenes el silencio improvisando.\nREGLAS OBLIGATORIAS PARA AGENDAR: después de ofrecer opciones de día o franja, quedate en silencio y no avances hasta recibir una elección clara. No elijas por la persona. Si responde con una duda, una muletilla o una frase incompleta, decí solamente “Claro, te escucho” y esperá nuevamente. Antes de confirmar que un asesor con más experiencia lo volverá a llamar, obtené el nombre de la persona, el día y la franja. Obtené también el nombre de la empresa o emprendimiento, salvo que la persona haya dicho que no tiene tiempo o que el responsable no estaba; en esos casos no prolongues la conversación para pedirlo. Preguntá el cargo de forma natural cuando corresponda; si prefiere no informarlo, podés continuar. Si dice que tiene web, Instagram o Facebook, pedí la dirección o usuario concreto, pero aceptá que prefiera no darlo. Confirmá en voz alta los datos obtenidos, el día y la franja. Cerrá brevemente usando el día y la franja confirmados: “Perfecto. Un asesor te llama el [día] por la [franja] y lo ven con más detalle”.${call.wrapUpRequested ? '\nCIERRE POR TIEMPO: quedan aproximadamente dos minutos antes del máximo configurado. No abras temas nuevos. Priorizá acordar la devolución del asesor, confirmar los datos necesarios y despedirte brevemente.' : ''}`,
        input: history,
        stream: true,
        max_output_tokens: 80,
      });
      let answer = '';
      let pendingToken = '';
      for await (const chunk of stream) {
        if (chunk.type === 'response.output_text.delta') {
          answer += chunk.delta;
          if (pendingToken) ws.send(JSON.stringify({ type: 'text', token: speechToken(pendingToken), last: false, interruptible: true }));
          pendingToken = chunk.delta;
        }
      }
      if (pendingToken) ws.send(JSON.stringify({ type: 'text', token: speechToken(pendingToken), last: true, interruptible: true }));
      history.push({ role: 'assistant', content: answer });
      if (history.length > 24) history.splice(0, history.length - 24);
      call.transcript.push({ user: userText, assistant: answer, at: Date.now() });
      call.updatedAt = Date.now();
      saveCallProgress(call.reference, call).catch(() => {});
      if (/\b(hasta luego|que tengas (?:un )?buen(?:o)? (?:día|tarde)|chau|adiós|me despido|gracias por tu tiempo)\b/i.test(answer)) endAfterSpeech(ws, call, answer);
      await ensureAppointment(call, history);
    } catch (error) {
      ws.send(JSON.stringify({ type: 'text', token: speechToken('Disculpá, tuve un problema técnico. La llamada finalizará.'), last: true }));
      call.error = error?.status === 401 ? 'OpenAI rechazó la credencial.' : error?.status === 429 ? 'OpenAI no tiene cuota disponible.' : 'No se pudo generar la respuesta.';
      call.updatedAt = Date.now();
      console.error('Error de IA durante llamada:', error?.status || error?.code || 'unknown');
    } finally {
      responding = false;
    }
  });
  ws.on('close', () => { if (wrapUpTimer) clearTimeout(wrapUpTimer); if (finalFarewellTimer) clearTimeout(finalFarewellTimer); });
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

async function runAutomationTick() {
  if (automationTickBusy || !databaseConfigured()) return;
  automationTickBusy = true;
  try {
    const settings = await getAutomationSettings();
    if (settings.status !== 'running' || !settings.fixed_message || !settings.instructions || !automationAllowedNow(settings)) return;
    const stats = await automationStats();
    if (stats.calls_last_ten_minutes >= settings.max_per_ten_minutes || stats.calls_today >= settings.daily_max || stats.active_contacts >= settings.concurrency) return;
    if (Date.now() - lastAutomationLaunchAt < settings.delay_seconds * 1000) return;
    const contact = await claimNextAutomationContact(settings.max_attempts);
    if (!contact) {
      lastAutomationEvent = { type:'waiting', message:'No se encontró un contacto pendiente elegible.', at:Date.now(), phone:null };
      return;
    }
    lastAutomationLaunchAt = Date.now();
    lastAutomationEvent = { type:'starting', message:'Enviando la llamada a Twilio…', at:Date.now(), phone:contact.phone };
    try {
      const started = await startOutboundCall({ to:contact.phone, contactId:contact.id, fixedMessage:settings.fixed_message, instructions:settings.instructions, automated:true, maxCallSeconds:settings.max_call_minutes*60 });
      lastAutomationEvent = { type:'started', message:`Llamada entregada a Twilio. Referencia ${started.reference}.`, at:Date.now(), phone:contact.phone };
    } catch (error) {
      await releaseAutomationContact(contact.id, 'failed');
      lastAutomationEvent = { type:'error', message:safeCallError(error), detail:String(error?.message || error?.code || 'Error desconocido').slice(0,300), at:Date.now(), phone:contact.phone };
      console.error('Error iniciando llamada automática:', error?.code || error?.message || 'unknown');
    }
  } catch (error) {
    lastAutomationEvent = { type:'error', message:'El motor automático tuvo un error interno.', detail:String(error?.message || 'Error desconocido').slice(0,300), at:Date.now(), phone:null };
    console.error('Error en motor automático:', error?.message || 'unknown');
  }
  finally { automationTickBusy = false; }
}

setInterval(runAutomationTick, 5000).unref();

app.use((error, _req, res, _next) => {
  if (error instanceof multer.MulterError) {
    const message = error.code === 'LIMIT_FILE_SIZE' ? 'El archivo supera el límite de 2 MB.' : 'No se pudo recibir el archivo.';
    return res.status(400).json({ error: message });
  }
  console.error('Error HTTP no controlado:', error?.message || 'unknown');
  res.status(500).json({ error: 'Ocurrió un error inesperado.' });
});

initializeDatabase()
  .then(async () => { await recoverInterruptedContacts(); server.listen(port, () => console.log(`Llamador IA disponible en http://localhost:${port}`)); })
  .catch((error) => { console.error('No se pudo inicializar PostgreSQL:', error.message); process.exit(1); });
