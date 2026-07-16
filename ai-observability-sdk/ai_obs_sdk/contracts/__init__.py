"""Vendored copy of the frozen observability contract (observability-iac/contracts).

Do not edit here — regenerate from the IaC repo when schema_version bumps.
"""
from .event_schema import ObsEvent
from .event_types import VALUES as EVENT_TYPE_VALUES, EventType
from .service_names import VALUES as SERVICE_NAME_VALUES, ServiceName

__all__ = [
    "ObsEvent",
    "EventType",
    "EVENT_TYPE_VALUES",
    "ServiceName",
    "SERVICE_NAME_VALUES",
]
