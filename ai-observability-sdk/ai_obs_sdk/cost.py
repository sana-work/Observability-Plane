"""Producer-side token cost estimate.

This is an *estimate* attached at emit time so dashboards work even before
enrichment; the Enrichment Consumer's Cost Calculator stage (authoritative
pricing from the control plane) overwrites it downstream.
"""
from __future__ import annotations

# (input_usd_per_1k_tokens, output_usd_per_1k_tokens) — keep in sync with
# observability.metric_catalog pricing rows; unknown models use DEFAULT.
PRICING: dict[str, tuple[float, float]] = {
    "gemini-1.5-pro": (0.00125, 0.005),
    "gemini-1.5-flash": (0.000075, 0.0003),
    "gemini-2.0-flash": (0.0001, 0.0004),
    "claude-sonnet-4-5": (0.003, 0.015),
    "claude-haiku-4-5": (0.001, 0.005),
    "llama-3-70b": (0.00265, 0.0035),
    "text-embedding-004": (0.000025, 0.0),
}
DEFAULT = (0.005, 0.015)


def estimate_cost_usd(model_name: str, input_tokens: int | None, output_tokens: int | None) -> float:
    in_price, out_price = PRICING.get(model_name, DEFAULT)
    return round(
        (input_tokens or 0) / 1000 * in_price + (output_tokens or 0) / 1000 * out_price, 8
    )
