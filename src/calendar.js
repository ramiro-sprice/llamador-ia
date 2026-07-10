import Holidays from 'date-holidays';

const holidays = new Holidays('AR');
const zone = 'America/Argentina/Buenos_Aires';

function argentinaParts(date = new Date()) {
  const parts = Object.fromEntries(new Intl.DateTimeFormat('en-CA', {
    timeZone: zone, year: 'numeric', month: '2-digit', day: '2-digit', hour: '2-digit', hourCycle: 'h23', weekday: 'long',
  }).formatToParts(date).filter((part) => part.type !== 'literal').map((part) => [part.type, part.value]));
  return { ...parts, hour: Number(parts.hour), isoDate: `${parts.year}-${parts.month}-${parts.day}` };
}

function addDays(isoDate, days) {
  const date = new Date(`${isoDate}T12:00:00-03:00`);
  date.setUTCDate(date.getUTCDate() + days);
  return new Intl.DateTimeFormat('en-CA', { timeZone: zone, year: 'numeric', month: '2-digit', day: '2-digit' }).format(date);
}

function businessDay(isoDate) {
  const date = new Date(`${isoDate}T12:00:00-03:00`);
  const weekday = new Intl.DateTimeFormat('en-US', { timeZone: zone, weekday: 'short' }).format(date);
  return weekday !== 'Sat' && weekday !== 'Sun' && !holidays.isHoliday(isoDate);
}

export function schedulingContext(now = new Date()) {
  const current = argentinaParts(now);
  const days = [];
  let offset = current.hour >= 16 ? 1 : 0;
  while (days.length < 4 && offset < 15) {
    const isoDate = addDays(current.isoDate, offset++);
    if (!businessDay(isoDate)) continue;
    const label = new Intl.DateTimeFormat('es-AR', { timeZone: zone, weekday: 'long', day: 'numeric', month: 'long' }).format(new Date(`${isoDate}T12:00:00-03:00`));
    const periods = isoDate === current.isoDate ? (current.hour < 12 ? ['tarde'] : current.hour < 16 ? ['tarde'] : []) : ['mañana', 'tarde'];
    if (periods.length) days.push({ isoDate, label, periods });
  }
  return `FECHA Y HORA ACTUAL EN ARGENTINA: ${current.isoDate}, ${current.hour}:00.\nOPCIONES HÁBILES VÁLIDAS: ${days.map((day) => `${day.label} (${day.isoDate}): ${day.periods.join(' o ')}`).join('; ')}. Mañana significa 09:00 a 12:00 y tarde 14:00 a 18:00. Ofrecé dos opciones concretas de esta lista. No ofrezcas fechas distintas.`;
}

export async function createCalendarEvent(data) {
  const url = process.env.GOOGLE_CALENDAR_WEBHOOK_URL;
  const secret = process.env.GOOGLE_CALENDAR_WEBHOOK_SECRET;
  if (!url || !secret) return { ok: false, error: 'CALENDAR_NOT_CONFIGURED' };
  const response = await fetch(url, {
    method: 'POST',
    headers: { 'content-type': 'application/json' },
    body: JSON.stringify({ ...data, secret }),
    redirect: 'follow',
  });
  const text = await response.text();
  try { return JSON.parse(text); }
  catch { return { ok: false, error: `Respuesta inválida del calendario (${response.status})` }; }
}

export function calendarConfigured() {
  return Boolean(process.env.GOOGLE_CALENDAR_WEBHOOK_URL && process.env.GOOGLE_CALENDAR_WEBHOOK_SECRET);
}
