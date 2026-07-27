"""obs-enrichment-consumer — Phase 3 of the Observability Plane.

Consumes ai-obs-events-raw, runs the 9-stage enrichment pipeline, produces
ai-obs-events-processed; invalid events go to ai-obs-dead-letter.
"""
__version__ = "0.1.0"
