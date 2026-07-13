import pg from 'pg';

const { Pool } = pg;
const pool = process.env.DATABASE_URL
  ? new Pool({ connectionString: process.env.DATABASE_URL, ssl: process.env.DATABASE_URL.includes('localhost') ? false : { rejectUnauthorized: false }, max: 5 })
  : null;

export function databaseConfigured() {
  return Boolean(pool);
}

export async function initializeDatabase() {
  if (!pool) return;
  await pool.query(`
    CREATE TABLE IF NOT EXISTS contacts (
      id UUID PRIMARY KEY,
      phone TEXT NOT NULL UNIQUE,
      person_name TEXT,
      company_name TEXT,
      keywords TEXT,
      role TEXT,
      website TEXT,
      instagram TEXT,
      facebook TEXT,
      notes TEXT,
      action TEXT NOT NULL DEFAULT 'LLAMADA_INICIAL',
      attempts INTEGER NOT NULL DEFAULT 0,
      source TEXT NOT NULL DEFAULT 'manual',
      referred_by UUID REFERENCES contacts(id) ON DELETE SET NULL,
      consent_status TEXT NOT NULL DEFAULT 'unknown',
      status TEXT NOT NULL DEFAULT 'pending',
      preferred_day TEXT,
      preferred_period TEXT,
      last_called_at TIMESTAMPTZ,
      next_call_at TIMESTAMPTZ,
      created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
      updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
    );
    CREATE INDEX IF NOT EXISTS contacts_status_idx ON contacts(status);
    CREATE INDEX IF NOT EXISTS contacts_next_call_idx ON contacts(next_call_at);
    ALTER TABLE contacts ADD COLUMN IF NOT EXISTS action TEXT NOT NULL DEFAULT 'LLAMADA_INICIAL';
    ALTER TABLE contacts ADD COLUMN IF NOT EXISTS attempts INTEGER NOT NULL DEFAULT 0;
    ALTER TABLE contacts ADD COLUMN IF NOT EXISTS keywords TEXT;
    CREATE TABLE IF NOT EXISTS call_records (
      id UUID PRIMARY KEY,
      reference UUID NOT NULL UNIQUE,
      contact_id UUID REFERENCES contacts(id) ON DELETE SET NULL,
      twilio_sid TEXT,
      recording_sid TEXT,
      recording_status TEXT,
      recording_duration INTEGER,
      status TEXT NOT NULL,
      summary TEXT,
      transcript JSONB NOT NULL DEFAULT '[]'::jsonb,
      started_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
      ended_at TIMESTAMPTZ,
      created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
    );
    ALTER TABLE call_records ADD COLUMN IF NOT EXISTS recording_sid TEXT;
    ALTER TABLE call_records ADD COLUMN IF NOT EXISTS recording_status TEXT;
    ALTER TABLE call_records ADD COLUMN IF NOT EXISTS recording_duration INTEGER;
    CREATE TABLE IF NOT EXISTS automation_settings (
      id INTEGER PRIMARY KEY CHECK (id = 1),
      status TEXT NOT NULL DEFAULT 'paused',
      weekdays JSONB NOT NULL DEFAULT '[1,2,3,4,5]'::jsonb,
      morning_start TIME NOT NULL DEFAULT '09:00',
      morning_end TIME NOT NULL DEFAULT '13:00',
      afternoon_start TIME NOT NULL DEFAULT '14:00',
      afternoon_end TIME NOT NULL DEFAULT '18:00',
      max_per_ten_minutes INTEGER NOT NULL DEFAULT 5,
      concurrency INTEGER NOT NULL DEFAULT 1,
      daily_max INTEGER NOT NULL DEFAULT 50,
      delay_seconds INTEGER NOT NULL DEFAULT 30,
      max_attempts INTEGER NOT NULL DEFAULT 3,
      retry_hours INTEGER NOT NULL DEFAULT 24,
      max_call_minutes INTEGER NOT NULL DEFAULT 8,
      fixed_message TEXT,
      instructions TEXT,
      started_at TIMESTAMPTZ,
      paused_at TIMESTAMPTZ,
      stopped_at TIMESTAMPTZ,
      updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
    );
    INSERT INTO automation_settings (id) VALUES (1) ON CONFLICT (id) DO NOTHING;
    ALTER TABLE automation_settings ADD COLUMN IF NOT EXISTS fixed_message TEXT;
    ALTER TABLE automation_settings ADD COLUMN IF NOT EXISTS instructions TEXT;
    ALTER TABLE automation_settings ADD COLUMN IF NOT EXISTS started_at TIMESTAMPTZ;
    ALTER TABLE automation_settings ADD COLUMN IF NOT EXISTS paused_at TIMESTAMPTZ;
    ALTER TABLE automation_settings ADD COLUMN IF NOT EXISTS stopped_at TIMESTAMPTZ;
    ALTER TABLE automation_settings ADD COLUMN IF NOT EXISTS max_call_minutes INTEGER NOT NULL DEFAULT 8;
  `);
}

export async function getAutomationSettings() {
  const { rows } = await pool.query('SELECT * FROM automation_settings WHERE id=1');
  return rows[0];
}

export async function saveAutomationSettings(settings) {
  const { rows } = await pool.query(`UPDATE automation_settings SET weekdays=$1::jsonb, morning_start=$2, morning_end=$3, afternoon_start=$4, afternoon_end=$5, max_per_ten_minutes=$6, concurrency=$7, daily_max=$8, delay_seconds=$9, max_attempts=$10, retry_hours=$11, max_call_minutes=$12, updated_at=NOW() WHERE id=1 RETURNING *`, [JSON.stringify(settings.weekdays), settings.morningStart, settings.morningEnd, settings.afternoonStart, settings.afternoonEnd, settings.maxPerTenMinutes, settings.concurrency, settings.dailyMax, settings.delaySeconds, settings.maxAttempts, settings.retryHours, settings.maxCallMinutes]);
  return rows[0];
}

export async function setAutomationState(status, templates = {}) {
  const timestampColumn = status === 'running' ? 'started_at' : status === 'paused' ? 'paused_at' : 'stopped_at';
  const { rows } = await pool.query(`UPDATE automation_settings SET status=$1, fixed_message=COALESCE($2,fixed_message), instructions=COALESCE($3,instructions), ${timestampColumn}=NOW(), updated_at=NOW() WHERE id=1 RETURNING *`, [status, templates.fixedMessage || null, templates.instructions || null]);
  return rows[0];
}

export async function automationStats() {
  const { rows } = await pool.query(`SELECT
    COUNT(*) FILTER (WHERE c.status IN ('pending','callback') AND c.consent_status <> 'denied' AND (c.next_call_at IS NULL OR c.next_call_at <= NOW()))::int AS eligible,
    (SELECT COUNT(*)::int FROM call_records a WHERE a.status IN ('queued','initiated','ringing','answered','in-progress')) AS active_contacts,
    (SELECT COUNT(*)::int FROM call_records r WHERE r.started_at >= NOW()-INTERVAL '10 minutes') AS calls_last_ten_minutes,
    (SELECT COUNT(*)::int FROM call_records r WHERE r.started_at >= date_trunc('day',NOW() AT TIME ZONE 'America/Argentina/Buenos_Aires') AT TIME ZONE 'America/Argentina/Buenos_Aires') AS calls_today
    FROM contacts c`);
  return rows[0];
}

export async function claimNextAutomationContact(maxAttempts) {
  const client = await pool.connect();
  try {
    await client.query('BEGIN');
    const { rows } = await client.query(`SELECT * FROM contacts WHERE status IN ('pending','callback') AND consent_status <> 'denied' AND attempts < $1 AND (next_call_at IS NULL OR next_call_at <= NOW()) ORDER BY COALESCE(next_call_at,created_at),created_at FOR UPDATE SKIP LOCKED LIMIT 1`, [maxAttempts]);
    if (!rows[0]) { await client.query('COMMIT'); return null; }
    await client.query(`UPDATE contacts SET status='in-progress',updated_at=NOW() WHERE id=$1`, [rows[0].id]);
    await client.query('COMMIT');
    return rows[0];
  } catch (error) { await client.query('ROLLBACK'); throw error; }
  finally { client.release(); }
}

export async function releaseAutomationContact(id, status = 'pending') {
  if (!pool || !id) return;
  await pool.query(`UPDATE contacts SET status=$1,updated_at=NOW() WHERE id=$2 AND status='in-progress'`, [status, id]);
}

export async function recoverInterruptedContacts() {
  if (!pool) return 0;
  const { rowCount } = await pool.query(`UPDATE contacts SET status='callback',updated_at=NOW() WHERE status='in-progress'`);
  return rowCount;
}

export async function listContacts() {
  const { rows } = await pool.query(`SELECT c.*, latest.started_at AS last_call_started_at, latest.ended_at AS last_call_ended_at, latest.status AS last_call_status, latest.summary AS last_call_summary, COALESCE(history.call_history, '[]'::json) AS call_history
    FROM contacts c
    LEFT JOIN LATERAL (SELECT started_at, ended_at, status, summary FROM call_records WHERE contact_id=c.id ORDER BY started_at DESC LIMIT 1) latest ON TRUE
    LEFT JOIN LATERAL (SELECT json_agg(json_build_object('startedAt',r.started_at,'endedAt',r.ended_at,'status',r.status,'summary',r.summary,'transcript',r.transcript) ORDER BY r.started_at DESC) AS call_history FROM call_records r WHERE r.contact_id=c.id) history ON TRUE
    ORDER BY COALESCE(c.next_call_at, c.created_at) ASC LIMIT 500`);
  return rows;
}

export async function createContact(contact) {
  const { rows } = await pool.query(`
    INSERT INTO contacts (id, phone, person_name, company_name, keywords, role, website, instagram, facebook, notes, source, referred_by, consent_status, status)
    VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14)
    RETURNING *
  `, [contact.id, contact.phone, contact.personName || null, contact.companyName || null, contact.keywords || null, contact.role || null, contact.website || null, contact.instagram || null, contact.facebook || null, contact.notes || null, contact.source || 'manual', contact.referredBy || null, contact.consentStatus || 'unknown', contact.status || 'pending']);
  return rows[0];
}

export async function importContacts(contacts) {
  const client = await pool.connect();
  const result = { created: 0, updated: 0, rejected: [] };
  try {
    await client.query('BEGIN');
    for (const [index, contact] of contacts.entries()) {
      try {
        const existing = await client.query('SELECT id FROM contacts WHERE phone=$1', [contact.phone]);
        if (existing.rowCount) {
          await client.query(`UPDATE contacts SET person_name=COALESCE(NULLIF($1,''),person_name), company_name=COALESCE(NULLIF($2,''),company_name), keywords=COALESCE(NULLIF($3,''),keywords), role=COALESCE(NULLIF($4,''),role), website=COALESCE(NULLIF($5,''),website), instagram=COALESCE(NULLIF($6,''),instagram), facebook=COALESCE(NULLIF($7,''),facebook), notes=COALESCE(NULLIF($8,''),notes), consent_status=$9, action=$10, updated_at=NOW() WHERE phone=$11`, [contact.personName, contact.companyName, contact.keywords, contact.role, contact.website, contact.instagram, contact.facebook, contact.notes, contact.consentStatus, contact.action, contact.phone]);
          result.updated++;
        } else {
          await client.query(`INSERT INTO contacts (id,phone,person_name,company_name,keywords,role,website,instagram,facebook,notes,consent_status,action,status,source) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,'pending','import')`, [contact.id, contact.phone, contact.personName, contact.companyName, contact.keywords, contact.role, contact.website, contact.instagram, contact.facebook, contact.notes, contact.consentStatus, contact.action]);
          result.created++;
        }
      } catch (error) { result.rejected.push({ row: index + 2, error: error.message }); }
    }
    await client.query('COMMIT');
    return result;
  } catch (error) { await client.query('ROLLBACK'); throw error; }
  finally { client.release(); }
}

export async function updateContact(id, fields) {
  const allowed = { personName:'person_name', companyName:'company_name', keywords:'keywords', role:'role', website:'website', instagram:'instagram', facebook:'facebook', notes:'notes', consentStatus:'consent_status', status:'status', action:'action', preferredDay:'preferred_day', preferredPeriod:'preferred_period', nextCallAt:'next_call_at' };
  const entries = Object.entries(fields).filter(([key]) => allowed[key]);
  if (!entries.length) return null;
  const values = entries.map(([, value]) => value === '' ? null : value);
  const sets = entries.map(([key], index) => `${allowed[key]}=$${index + 1}`);
  values.push(id);
  const { rows } = await pool.query(`UPDATE contacts SET ${sets.join(',')}, updated_at=NOW() WHERE id=$${values.length} RETURNING *`, values);
  return rows[0] || null;
}

export async function getContact(id) {
  if (!pool || !id) return null;
  const { rows } = await pool.query(`SELECT c.*, COALESCE(history.notes_history, '') AS notes_history FROM contacts c LEFT JOIN LATERAL (
    SELECT string_agg(to_char(r.started_at AT TIME ZONE 'America/Argentina/Buenos_Aires','DD/MM/YYYY HH24:MI') || ' — ' || COALESCE(r.summary,r.status), E'\n' ORDER BY r.started_at DESC) AS notes_history
    FROM call_records r WHERE r.contact_id=c.id
  ) history ON TRUE WHERE c.id=$1`, [id]);
  return rows[0] || null;
}

export async function deleteContacts(ids) {
  if (!ids.length) return 0;
  const { rowCount } = await pool.query('DELETE FROM contacts WHERE id = ANY($1::uuid[])', [ids]);
  return rowCount;
}

export async function saveCallStart(reference, contactId, sid, status) {
  if (!pool) return;
  await pool.query(`INSERT INTO call_records (id, reference, contact_id, twilio_sid, status) VALUES ($1,$2,$3,$4,$5) ON CONFLICT (reference) DO NOTHING`, [reference, reference, contactId || null, sid || null, status]);
  if (contactId) await pool.query(`UPDATE contacts SET last_called_at=NOW(), status='in-progress', attempts=attempts+1, updated_at=NOW() WHERE id=$1`, [contactId]);
}

export async function saveCallProgress(reference, call) {
  if (!pool) return;
  const transcript = call.transcript || [];
  const summary = call.appointment?.summary || transcript.slice(-3).map((turn) => turn.user).filter(Boolean).join(' ').slice(0, 600) || call.error || null;
  await pool.query(`UPDATE call_records SET status=$1, transcript=$2::jsonb, summary=$3, ended_at=CASE WHEN $1 IN ('completed','failed','busy','no-answer','canceled') THEN COALESCE(ended_at,NOW()) ELSE ended_at END WHERE reference=$4`, [call.status, JSON.stringify(transcript), summary, reference]);
  if (call.contactId) {
    const contactStatus = call.appointment?.eventId ? 'scheduled'
      : call.status === 'no-answer' || call.status === 'busy' || call.status === 'canceled' ? 'no-answer'
      : call.status === 'failed' ? 'failed'
      : call.status === 'completed' ? 'in-progress'
      : null;
    if (contactStatus) await pool.query(`UPDATE contacts SET status=$1, updated_at=NOW() WHERE id=$2 AND status NOT IN ('scheduled','not-interested')`, [contactStatus, call.contactId]);
  }
}

export async function saveCallRecording(reference, recording) {
  if (!pool) return;
  await pool.query(`UPDATE call_records SET recording_sid=$1, recording_status=$2, recording_duration=$3 WHERE reference=$4`, [
    recording.sid || null,
    recording.status || null,
    Number.isFinite(recording.duration) ? recording.duration : null,
    reference,
  ]);
}

export async function closeDatabase() {
  if (pool) await pool.end();
}
