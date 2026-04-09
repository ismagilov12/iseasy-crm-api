-- ═══════════════════════════════════════════════════
--  IS EASY CRM — AI Agent layer tables
--  Запусти в Supabase SQL Editor ПЕРЕД деплоем api/agent.py
-- ═══════════════════════════════════════════════════

-- 1. Полный лог всех вызовов агента (неизменяемый, только append)
CREATE TABLE IF NOT EXISTS agent_actions (
  id BIGSERIAL PRIMARY KEY,
  tool_name TEXT NOT NULL,
  params JSONB DEFAULT '{}',
  mode TEXT NOT NULL,              -- auto / confirm
  status TEXT NOT NULL,            -- executed / pending / rejected / failed
  result JSONB DEFAULT '{}',
  error TEXT DEFAULT '',
  pending_id BIGINT,               -- если действие прошло через очередь — id в pending_actions
  created_at TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_agent_actions_tool ON agent_actions(tool_name);
CREATE INDEX IF NOT EXISTS idx_agent_actions_status ON agent_actions(status);
CREATE INDEX IF NOT EXISTS idx_agent_actions_created ON agent_actions(created_at DESC);

-- 2. Очередь действий на подтверждение менеджером
CREATE TABLE IF NOT EXISTS pending_actions (
  id BIGSERIAL PRIMARY KEY,
  tool_name TEXT NOT NULL,
  params JSONB DEFAULT '{}',
  status TEXT NOT NULL DEFAULT 'pending',  -- pending / approved / rejected / expired / executed
  reason TEXT DEFAULT '',                  -- почему агент попросил подтверждения
  preview JSONB DEFAULT '{}',              -- краткое превью для UI (что именно произойдёт)
  approved_by TEXT DEFAULT '',
  approved_at TIMESTAMPTZ,
  executed_at TIMESTAMPTZ,
  expires_at TIMESTAMPTZ,
  created_at TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_pending_status ON pending_actions(status);
CREATE INDEX IF NOT EXISTS idx_pending_created ON pending_actions(created_at DESC);
CREATE INDEX IF NOT EXISTS idx_pending_expires ON pending_actions(expires_at);

-- 3. RLS
ALTER TABLE agent_actions ENABLE ROW LEVEL SECURITY;
ALTER TABLE pending_actions ENABLE ROW LEVEL SECURITY;

CREATE POLICY "Allow all for anon" ON agent_actions FOR ALL USING (true) WITH CHECK (true);
CREATE POLICY "Allow all for anon" ON pending_actions FOR ALL USING (true) WITH CHECK (true);

-- Готово!
