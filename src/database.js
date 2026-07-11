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
    CREATE TABLE IF NOT EXISTS call_records (
      id UUID PRIMARY KEY,
      reference UUID NOT NULL UNIQUE,
      contact_id UUID REFERENCES contacts(id) ON DELETE SET NULL,
      twilio_sid TEXT,
      status TEXT NOT NULL,
      summary TEXT,
      transcript JSONB NOT NULL DEFAULT '[]'::jsonb,
      started_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
      ended_at TIMESTAMPTZ,
      created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
    );
  `);
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
    INSERT INTO contacts (id, phone, person_name, company_name, role, website, instagram, facebook, notes, source, referred_by, consent_status, status)
    VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13)
    RETURNING *
  `, [contact.id, contact.phone, contact.personName || null, contact.companyName || null, contact.role || null, contact.website || null, contact.instagram || null, contact.facebook || null, contact.notes || null, contact.source || 'manual', contact.referredBy || null, contact.consentStatus || 'unknown', contact.status || 'pending']);
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
          await client.query(`UPDATE contacts SET person_name=COALESCE(NULLIF($1,''),person_name), company_name=COALESCE(NULLIF($2,''),company_name), role=COALESCE(NULLIF($3,''),role), website=COALESCE(NULLIF($4,''),website), instagram=COALESCE(NULLIF($5,''),instagram), facebook=COALESCE(NULLIF($6,''),facebook), notes=COALESCE(NULLIF($7,''),notes), consent_status=$8, action=$9, updated_at=NOW() WHERE phone=$10`, [contact.personName, contact.companyName, contact.role, contact.website, contact.instagram, contact.facebook, contact.notes, contact.consentStatus, contact.action, contact.phone]);
          result.updated++;
        } else {
          await client.query(`INSERT INTO contacts (id,phone,person_name,company_name,role,website,instagram,facebook,notes,consent_status,action,status,source) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,'pending','import')`, [contact.id, contact.phone, contact.personName, contact.companyName, contact.role, contact.website, contact.instagram, contact.facebook, contact.notes, contact.consentStatus, contact.action]);
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
  const allowed = { personName:'person_name', companyName:'company_name', role:'role', website:'website', instagram:'instagram', facebook:'facebook', notes:'notes', consentStatus:'consent_status', status:'status', action:'action', preferredDay:'preferred_day', preferredPeriod:'preferred_period', nextCallAt:'next_call_at' };
  const entries = Object.entries(fields).filter(([key]) => allowed[key]);
  if (!entries.length) return null;
  const values = entries.map(([, value]) => value === '' ? null : value);
  const sets = entries.map(([key], index) => `${allowed[key]}=$${index + 1}`);
  values.push(id);
  const { rows } = await pool.query(`UPDATE contacts SET ${sets.join(',')}, updated_at=NOW() WHERE id=$${values.length} RETURNING *`, values);
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
    if (contactStatus) await pool.query(`UPDATE contacts SET status=$1, updated_at=NOW() WHERE id=$2 AND status <> 'scheduled'`, [contactStatus, call.contactId]);
  }
}

export async function closeDatabase() {
  if (pool) await pool.end();
}
