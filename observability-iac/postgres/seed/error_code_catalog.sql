-- Starter error-code mapping — the Enrichment Consumer maps raw exception strings to these.
INSERT INTO observability.error_code_catalog (raw_pattern, error_code, category) VALUES
  ('%ReadTimeout%',       'UPSTREAM_TIMEOUT',      'timeout'),
  ('%ConnectionRefused%', 'UPSTREAM_UNAVAILABLE',  'network'),
  ('%429%',               'RATE_LIMITED',          'throttling'),
  ('%SafetyBlock%',       'LLM_SAFETY_BLOCKED',    'guardrail'),
  ('%ValidationError%',   'SCHEMA_INVALID',        'validation'),
  ('%401%',               'UNAUTHENTICATED',       'auth'),
  ('%403%',               'FORBIDDEN',             'auth'),
  ('%pgvector%',          'VECTOR_QUERY_ERROR',    'retrieval')
ON CONFLICT (error_code) DO NOTHING;
