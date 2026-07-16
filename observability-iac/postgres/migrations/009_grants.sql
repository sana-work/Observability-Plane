-- Least-privilege grants per consumer role (roles created in 001).

-- enrichment consumer: read registries/catalogs/limits, write budgets + SLO compliance
GRANT USAGE ON SCHEMA observability TO obs_enrichment;
GRANT SELECT ON ALL TABLES IN SCHEMA observability TO obs_enrichment;
GRANT INSERT, UPDATE ON observability.budget_accumulator,
                        observability.daily_slo_compliance TO obs_enrichment;
GRANT EXECUTE ON FUNCTION observability.add_spend(VARCHAR, TEXT, NUMERIC) TO obs_enrichment;

-- storage consumer: write aggregates + feedback cases
GRANT USAGE ON SCHEMA observability TO obs_storage;
GRANT SELECT ON ALL TABLES IN SCHEMA observability TO obs_storage;
GRANT INSERT, UPDATE ON observability.agg_hourly_application_metrics,
                        observability.agg_hourly_agent_metrics,
                        observability.agg_hourly_tool_metrics,
                        observability.agg_hourly_llm_metrics,
                        observability.agg_hourly_rag_metrics,
                        observability.agg_daily_feedback_metrics,
                        observability.agg_daily_kpi_metric,
                        observability.feedback_case TO obs_storage;

-- dashboard/chatbot backend: read everything, manage config + prompts + cases
GRANT USAGE ON SCHEMA observability TO obs_dashboard;
GRANT SELECT ON ALL TABLES IN SCHEMA observability TO obs_dashboard;
GRANT INSERT, UPDATE ON observability.dashboard_config,
                        observability.prompt_template_registry,
                        observability.prompt_activation_audit,
                        observability.feedback_case,
                        observability.alert_threshold,
                        observability.budget_limits TO obs_dashboard;

-- read-only (chatbot query planner, BI)
GRANT USAGE ON SCHEMA observability TO dashboard_ro;
GRANT SELECT ON ALL TABLES IN SCHEMA observability TO dashboard_ro;
ALTER DEFAULT PRIVILEGES IN SCHEMA observability GRANT SELECT ON TABLES TO dashboard_ro;
