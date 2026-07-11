import crypto from 'node:crypto';
import readXlsxFile from 'read-excel-file/node';
import { parse } from 'csv-parse/sync';

const aliases = {
  nombre:'personName', persona:'personName', empresa:'companyName', negocio:'companyName', keywords:'keywords', palabras_clave:'keywords', 'palabras clave':'keywords', cargo:'role', telefono:'phone', teléfono:'phone', celular:'phone', web:'website', website:'website', instagram:'instagram', facebook:'facebook', notas:'notes', nota:'notes', consentimiento:'consentStatus', accion:'action', acción:'action',
};

function normalizeHeader(value) {
  return String(value || '').trim().toLowerCase().normalize('NFD').replace(/[\u0300-\u036f]/g, '');
}

function normalizePhone(value) {
  const phone = String(value || '').replace(/\.0$/, '').replace(/[\s()-]/g, '');
  return /^\+54\d{10,13}$/.test(phone) ? phone : null;
}

function mapRows(rows) {
  if (rows.length < 2) return { contacts: [], rejected: [{ row: 1, error: 'El archivo no contiene datos.' }] };
  const headers = rows[0].map((header) => aliases[normalizeHeader(header)] || null);
  const contacts = []; const rejected = [];
  rows.slice(1).forEach((row, index) => {
    if (row.every((value) => String(value || '').trim() === '')) return;
    const raw = {};
    headers.forEach((header, column) => { if (header) raw[header] = String(row[column] ?? '').trim(); });
    const phone = normalizePhone(raw.phone);
    if (!phone) { rejected.push({ row: index + 2, error: 'Teléfono argentino inválido. Usá formato +54.' }); return; }
    const consent = normalizeHeader(raw.consentStatus);
    contacts.push({
      id: crypto.randomUUID(), phone, personName: raw.personName || '', companyName: raw.companyName || '', keywords: raw.keywords || '', role: raw.role || '', website: raw.website || '', instagram: raw.instagram || '', facebook: raw.facebook || '', notes: raw.notes || '',
      consentStatus: ['si','sí','autorizado','granted'].includes(consent) ? 'granted' : ['no','denegado','denied'].includes(consent) ? 'denied' : 'unknown',
      action: String(raw.action || 'LLAMADA_INICIAL').toUpperCase().replace(/\s+/g, '_'),
    });
  });
  return { contacts, rejected };
}

export async function parseContactFile(file) {
  if (/\.csv$/i.test(file.originalname)) return mapRows(parse(file.buffer, { bom: true, skip_empty_lines: true, relax_column_count: true }));
  if (/\.xlsx$/i.test(file.originalname)) return mapRows(await readXlsxFile(file.buffer));
  throw new Error('Formato no permitido. Usá .xlsx o .csv.');
}
