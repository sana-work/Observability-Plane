-- Seed: baseline error taxonomy for enrichment stage 5.
-- Prefixes: P=platform, A=agent, L=llm, T=tool, R=rag, G=guardrail, K=kafka.
-- match_pattern is matched (case-insensitive) against "<ExceptionClass>: <message>".

INSERT INTO observability.error_code_catalog
  (error_code, category, title, match_pattern, priority, severity, retryable) VALUES
  -- LLM
  ('L0001', 'llm',   'LLM rate limited',            '(RateLimit|429|ResourceExhausted)',            10, 'warning',  true),
  ('L0002', 'llm',   'LLM safety blocked',          '(SafetyError|blocked by safety|content filter)',10, 'warning',  false),
  ('L0003', 'llm',   'LLM context length exceeded', '(context.length|token limit|maximum context)', 10, 'error',    false),
  ('L0004', 'llm',   'LLM provider unavailable',    '(503|ServiceUnavailable|connection.*(vertex|model))', 20, 'critical', true),
  ('L0999', 'llm',   'LLM call failed (other)',     'LLM',                                          90, 'error',    false),
  -- Tool
  ('T0001', 'tool',  'Tool call timeout',           '(TimeoutError|ReadTimeout|deadline)',          10, 'error',    true),
  ('T0002', 'tool',  'Tool auth failure',           '(401|403|Unauthorized|Forbidden)',             10, 'error',    false),
  ('T0003', 'tool',  'Tool schema validation failed','(ValidationError|invalid input schema)',      10, 'error',    false),
  ('T0999', 'tool',  'Tool call failed (other)',    'Tool',                                         90, 'error',    false),
  -- Agent
  ('A0001', 'agent', 'Agent max steps exceeded',    '(max.steps|step limit|loop limit)',            10, 'error',    false),
  ('A0002', 'agent', 'Agent handoff failed',        'handoff',                                      20, 'error',    true),
  ('A0003', 'agent', 'Agent timeout',               '(AgentTimeout|execution deadline)',            10, 'error',    true),
  ('A0999', 'agent', 'Agent failed (other)',        'Agent',                                        90, 'error',    false),
  -- RAG
  ('R0001', 'rag',   'Vector index unavailable',    '(index unavailable|pgvector|vector.*connect)', 10, 'critical', true),
  ('R0002', 'rag',   'Embedding generation failed', '(embedding.*(fail|error))',                    10, 'error',    true),
  ('R0999', 'rag',   'RAG retrieval failed (other)','(RAG|retriev)',                                90, 'error',    false),
  -- Guardrail / platform / kafka
  ('G0001', 'guardrail', 'Guardrail policy violation', '(policy violation|guardrail)',              10, 'warning',  false),
  ('P0001', 'platform',  'Upstream HTTP 5xx',          '(500|502|504|Internal Server Error)',       50, 'error',    true),
  ('P0002', 'platform',  'Database error',             '(asyncpg|psycopg|OperationalError|deadlock)',20, 'critical', true),
  ('K0001', 'kafka',     'Kafka produce failure',      '(KafkaException|BufferError|broker)',       20, 'critical', true),
  ('P0999', 'platform',  'Uncategorised error',        '.*',                                       999, 'error',    false)
ON CONFLICT (error_code) DO UPDATE
  SET match_pattern = EXCLUDED.match_pattern, priority = EXCLUDED.priority;
