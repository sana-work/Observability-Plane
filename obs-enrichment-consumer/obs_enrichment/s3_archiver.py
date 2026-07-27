"""Stage-7 S3 offload of oversized payload fields.

Fields listed in OBS_ENRICH_ARCHIVE_FIELDS whose (post-redaction) text exceeds
the threshold are written to the archive bucket and replaced in the payload by
an s3:// pointer — Kafka messages stay small (raw topic caps at 1 MiB) and ES
docs stay lean.
"""
from __future__ import annotations

from .config import EnrichSettings

_PREFIX_BY_FIELD = {
    "prompt": "redacted-prompts",
    "response": "redacted-responses",
    "raw_response": "redacted-responses",
    "rag_context": "rag-contexts",
    "trace_json": "raw-traces",
}
_DEFAULT_PREFIX = "rag-contexts"


class S3Archiver:
    def __init__(self, settings: EnrichSettings):
        import boto3  # lazy: tests use a fake

        self._client = boto3.client("s3")
        self._bucket = settings.s3_bucket

    def put_text(self, key: str, text: str) -> str:
        self._client.put_object(
            Bucket=self._bucket, Key=key, Body=text.encode(), ContentType="text/plain"
        )
        return f"s3://{self._bucket}/{key}"


def archive_key(field: str, event_id: str) -> str:
    prefix = _PREFIX_BY_FIELD.get(field, _DEFAULT_PREFIX)
    return f"{prefix}/{event_id}/{field}.txt"
