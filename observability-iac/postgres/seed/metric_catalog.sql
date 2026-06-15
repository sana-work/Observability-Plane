-- 30 initial metrics. source_table points at the interim Postgres obs_events.* firehose
-- (swap to sf_* when Snowflake onboards — plan.md §15.6).
INSERT INTO observability.metric_catalog (metric_name, formula, source_table, unit) VALUES
-- golden signals
('request_count','count(*)','obs_events.events','count'),
('error_rate','errors/requests','obs_events.events','ratio'),
('latency_p50','percentile(latency_ms,50)','obs_events.events','ms'),
('latency_p95','percentile(latency_ms,95)','obs_events.events','ms'),
('latency_p99','percentile(latency_ms,99)','obs_events.events','ms'),
('throughput_rps','request_count/window_seconds','obs_events.events','rps'),
-- LLM
('llm_input_tokens','sum(input_tokens)','obs_events.llm_events','tokens'),
('llm_output_tokens','sum(output_tokens)','obs_events.llm_events','tokens'),
('llm_total_tokens','sum(total_tokens)','obs_events.llm_events','tokens'),
('llm_estimated_cost','sum(estimated_cost)','obs_events.llm_events','usd'),
('llm_latency_p95','percentile(llm_latency_ms,95)','obs_events.llm_events','ms'),
('llm_rate_limit_rate','rate_limited/llm_calls','obs_events.llm_events','ratio'),
('llm_safety_block_rate','safety_blocked/llm_calls','obs_events.llm_events','ratio'),
-- agent
('agent_success_rate','completed/started','obs_events.agent_events','ratio'),
('agent_avg_steps','avg(step_count)','obs_events.agent_events','count'),
('agent_loop_rate','loops/agent_runs','obs_events.agent_events','ratio'),
('agent_handoff_count','sum(handoff_count)','obs_events.agent_events','count'),
-- tool
('tool_success_rate','tool_completed/tool_calls','obs_events.events','ratio'),
('tool_latency_p95','percentile(tool_latency_ms,95)','obs_events.events','ms'),
('tool_timeout_rate','tool_timeouts/tool_calls','obs_events.events','ratio'),
-- RAG / quality
('rag_no_result_rate','no_result/retrievals','obs_events.rag_events','ratio'),
('rag_avg_chunks','avg(retrieved_chunk_count)','obs_events.rag_events','count'),
('rag_avg_relevance','avg(avg_relevance_score)','obs_events.rag_events','score'),
('faithfulness_score','avg(faithfulness_score)','obs_events.quality_scores','score'),
('hallucination_rate','avg(hallucination_flag)','obs_events.quality_scores','ratio'),
-- cost / budget
('budget_utilisation_pct','spend/max_spend_usd','observability.budget_limits','ratio'),
-- kafka
('kafka_consumer_lag','max(kafka_consumer_lag)','obs_events.events','count'),
-- SLO
('slo_error_budget_consumed','avg(error_budget_consumed_pct)','observability.daily_slo_compliance','ratio'),
('slo_burn_rate_1h','max(burn_rate_1h)','observability.daily_slo_compliance','ratio'),
-- feedback
('feedback_negative_rate','negative/total_feedback','obs_events.feedback_events','ratio')
ON CONFLICT (metric_name) DO NOTHING;
