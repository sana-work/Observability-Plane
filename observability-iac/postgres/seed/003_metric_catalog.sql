-- Seed: core metric semantic layer (chatbot + KPI dashboard). Extend per LOB.

INSERT INTO observability.metric_catalog
  (metric_id, metric_name, description, formula, source_system, source_object, unit, aggregation, dimensions, synonyms) VALUES
  ('request_count', 'Request count', 'Total platform requests',
   'count(*) WHERE event_type = ''REQUEST_COMPLETED''', 'obs_events', 'obs_events.events',
   'count', 'sum', '{application_id,lob,service_name}', '{requests,traffic,volume}'),
  ('error_rate_pct', 'Error rate', 'Failed requests as % of total',
   '100.0 * count(*) FILTER (WHERE event_type=''REQUEST_FAILED'') / NULLIF(count(*),0)',
   'obs_events', 'obs_events.events', 'pct', 'avg', '{application_id,lob}', '{errors,failure rate}'),
  ('p95_latency_ms', 'P95 latency', '95th percentile request latency',
   'percentile_cont(0.95) WITHIN GROUP (ORDER BY latency_ms)', 'obs_events', 'obs_events.events',
   'ms', 'p95', '{application_id,service_name}', '{latency,response time,slow}'),
  ('llm_cost_usd', 'LLM cost', 'Total estimated LLM spend',
   'sum(estimated_cost_usd)', 'obs_events', 'obs_events.llm_events',
   'usd', 'sum', '{application_id,model_name,agent_id}', '{cost,spend,budget,bill}'),
  ('llm_tokens_total', 'Total tokens', 'Input+output tokens consumed',
   'sum(total_tokens)', 'obs_events', 'obs_events.llm_events',
   'tokens', 'sum', '{application_id,model_name}', '{tokens,usage}'),
  ('agent_success_rate_pct', 'Agent success rate', 'Successful agent runs as % of total',
   '100.0 * count(*) FILTER (WHERE event_type=''AGENT_COMPLETED'') / NULLIF(count(*) FILTER (WHERE event_type IN (''AGENT_COMPLETED'',''AGENT_FAILED'',''AGENT_TIMEOUT'')),0)',
   'obs_events', 'obs_events.agent_events', 'pct', 'avg', '{agent_id,application_id}', '{agent success}'),
  ('rag_no_result_rate_pct', 'RAG no-result rate', 'Retrievals returning zero chunks as % of total',
   '100.0 * count(*) FILTER (WHERE no_result_flag) / NULLIF(count(*),0)',
   'obs_events', 'obs_events.rag_events', 'pct', 'avg', '{rag_id}', '{no results,empty retrieval}'),
  ('rag_avg_relevance', 'RAG relevance', 'Mean retrieval relevance score',
   'avg(avg_relevance_score)', 'obs_events', 'obs_events.rag_events',
   'count', 'avg', '{rag_id}', '{relevance,retrieval quality}'),
  ('feedback_avg_rating', 'Average rating', 'Mean user feedback rating (1-5)',
   'avg(rating)', 'obs_events', 'obs_events.feedback_events',
   'count', 'avg', '{application_id,agent_id}', '{rating,satisfaction,csat}'),
  ('kafka_consumer_lag', 'Consumer lag', 'Kafka consumer group lag',
   'kminion_kafka_consumer_group_topic_lag', 'prometheus', 'kminion_kafka_consumer_group_topic_lag',
   'count', 'last', '{group_id,topic}', '{lag,backlog,behind}')
ON CONFLICT (metric_id) DO UPDATE
  SET formula = EXCLUDED.formula, description = EXCLUDED.description;

-- Model pricing — MUST stay in sync with ai_obs_sdk/cost.py PRICING (SDK gives
-- the producer-side estimate; this table is what enrichment bills with).
INSERT INTO observability.model_pricing (model_name, effective_from, input_usd_per_1k, output_usd_per_1k, provider) VALUES
  ('gemini-1.5-pro',      '2026-01-01', 0.00125,  0.005,  'vertexai'),
  ('gemini-1.5-flash',    '2026-01-01', 0.000075, 0.0003, 'vertexai'),
  ('gemini-2.0-flash',    '2026-01-01', 0.0001,   0.0004, 'vertexai'),
  ('claude-sonnet-4-5',   '2026-01-01', 0.003,    0.015,  'anthropic'),
  ('claude-haiku-4-5',    '2026-01-01', 0.001,    0.005,  'anthropic'),
  ('llama-3-70b',         '2026-01-01', 0.00265,  0.0035, 'r2d2'),
  ('text-embedding-004',  '2026-01-01', 0.000025, 0.0,    'vertexai')
ON CONFLICT (model_name, effective_from) DO UPDATE
  SET input_usd_per_1k = EXCLUDED.input_usd_per_1k,
      output_usd_per_1k = EXCLUDED.output_usd_per_1k;
