# User Feedback Service — Read API + Production-Readiness Enhancements
## Goal: reach Java CAAR parity + let the Custom Dashboard consume data via GET

The Python `genai-user-feedback` service is currently **write-only** with raw-string
logging. This doc closes the 10 gaps found against the Java CAAR service, in priority order.

### Gap → fix map

| # | Gap (vs CAAR) | Fix in this doc |
|---|---|---|
| 1 | No structured error codes | §2 Error code catalog + envelope |
| 2 | No uniform API response envelope | §1 `ApiResponse[T]` wrapper on every route |
| 3 | No feedback retrieval API (write-only) | §6 `GET /feedback`, `/feedback/{id}` |
| 4 | No status lifecycle | §3 `status` column (open→reviewed→fixed) + PATCH |
| 5 | No PII redaction | §8 redact `prompt`/`comments` in logs (`soe_id` retained per requirement) |
| 6 | No duplicate detection | §3 unique `(soe_id, correlation_id)` + §5 409 handling |
| 7 | No metrics endpoint | §7 Prometheus `/metrics` |
| 8 | No audit trail | §9 structured audit events (success + failure) |
| 9 | No analytics layer (`/getFeedBackSummary`, `/getReportProblemDetails`) | §6 `/feedback/summary`, `/feedback/problems` |
| 10 | Unstructured log messages | §8 all logs via `extra={}` key-values, not f-strings |

---

## Backlog traceability (E-P0 / E-P1 / E-P2)

Every backlog ID maps to a concrete section below. Risk / migration / effort carried from
the capability matrix. Effort: XS ≈ <2h, S ≈ ½–1 day, M ≈ 1–2 days, L ≈ 3+ days.

### P0 — must fix (basic observability + safety)

| ID | Capability | § | Risk | Migration | Effort |
|---|---|---|---|---|---|
| E-P0-1 | `feedback_id` in logs | §9 filter + §6 audit | None | No | XS |
| E-P0-2 | Auth failures logged | §8 `auth.py` | None | No | XS |
| E-P0-3 | DB errors logged | §6 `create()` except | None | No | XS |
| E-P0-4 | Uniform response envelope | §1 | **Medium** (changes success shape) | No | S |
| E-P0-5 | Structured error codes | §2 | Low | No | S |
| E-P0-6 | PII redaction in logs | §8 `_redact()` | None | No | S |
| E-P0-7 | Global exception handler | §8 handlers | Low | No | S |

### P1 — production readiness

| ID | Capability | § | Risk | Migration | Effort |
|---|---|---|---|---|---|
| E-P1-1 | `feedback_type` enum constraint | **§11 (new)** | **Medium** (rejects previously valid) | No | S |
| E-P1-2 | Duplicate prevention `(soe_id, correlation_id)` | §3 unique constraint | Low | Yes (unique idx) | S |
| E-P1-3 | `status` lifecycle field | §3 + §7 PATCH | None | Yes (column) | S |
| E-P1-4 | Feedback retrieval API | §7 GET routes | None | No | S |
| E-P1-5 | `trace_id` in logs | §9 filter | None | No | XS |
| E-P1-6 | `latency_ms` structured field | §8 middleware | None | No | XS |
| E-P1-7 | HTTP `status_code` in log | §8 middleware | None | No | XS |
| E-P1-8 | `environment` + `service_name` in logs | §9 filter | None | No | XS |
| E-P1-9 | Prometheus `/metrics` | §4 + §8 | None | No | S |
| E-P1-10 | Audit log on submission | §6 + §9 `audit` logger | None | No | S |

### P2 — nice-to-have

| ID | Capability | § | Risk | Migration | Effort |
|---|---|---|---|---|---|
| E-P2-1 | Analytics endpoints (`/summary`, `/by-type`) | §7 summary + **§12 (new)** | None | No | M |
| E-P2-2 | 1–5 Likert `rating` field | **§13 (new)** | None | Yes (column) | S |
| E-P2-3 | OpenTelemetry span export | **§14 (new, deferred)** | None | No | M |
| E-P2-4 | RBAC roles (admin vs submitter) | **§15 (new)** | **High** (changes auth) | No | L |
| E-P2-5 | OpenAPI contact/server metadata | **§16 (new)** | None | No | XS |
| E-P2-6 | ~~`user_hash` pseudonym~~ — **dropped** (raw `soe_id` retained per requirement, no hashing) | — | — | — | — |
| E-P2-7 | Deletion / anonymisation endpoint | **§17 (new)** | None | No | M |

> **Sequencing note:** do all P0 first (XS/S, mostly additive) — only E-P0-4 (envelope)
> is a breaking response-shape change, so coordinate it with the Custom Dashboard team.
> In P1, **E-P1-1 (`feedback_type` enum)** and in P2 **E-P2-4 (RBAC)** are the only items
> that can reject previously-accepted traffic — gate them behind a config flag and roll out
> in a permissive (log-only) mode first.

---

## Files to change

```
feedback/errors.py           ← NEW: error code enum + AppException
feedback/responses.py        ← NEW: ApiResponse envelope + helpers
feedback/metrics.py          ← NEW: Prometheus counters/histograms
feedback/schemas.py          ← add correlation_id (mandatory), status, created_at, updated_at + unique constraint
feedback/models.py           ← add read-side + envelope Pydantic models
feedback/repositories.py     ← add read methods + duplicate detection + audit logging
feedback/api/v1/api.py       ← add GET/PATCH routes, wrap all in envelope
feedback/main.py             ← register exception handlers + /metrics + PII redaction
alembic/versions/002_*.py    ← migration
```

---

## §1 — `feedback/responses.py` — uniform response envelope (Gap #2)

Every endpoint returns the **same shape** on success and error.

```python
# feedback/responses.py
from datetime import datetime, timezone
from typing import Generic, Optional, TypeVar

from pydantic import BaseModel
from pydantic.generics import GenericModel   # pydantic v1; for v2 use Generic[T] on BaseModel

T = TypeVar("T")


class ErrorDetail(BaseModel):
    code: str                      # structured, machine-readable (see errors.py)
    message: str                   # human-readable, safe to show
    detail: Optional[str] = None   # optional extra context (never PII)


class ApiResponse(GenericModel, Generic[T]):
    status: str                    # "success" | "error"
    data: Optional[T] = None
    error: Optional[ErrorDetail] = None
    correlation_id: str
    timestamp: str


def success(data, correlation_id: str) -> dict:
    return ApiResponse(
        status="success",
        data=data,
        correlation_id=correlation_id,
        timestamp=datetime.now(timezone.utc).isoformat(),
    ).dict()


def failure(code: str, message: str, correlation_id: str, detail: str | None = None) -> dict:
    return ApiResponse(
        status="error",
        error=ErrorDetail(code=code, message=message, detail=detail),
        correlation_id=correlation_id,
        timestamp=datetime.now(timezone.utc).isoformat(),
    ).dict()
```

> **Pydantic v2 note:** drop `GenericModel`; declare `class ApiResponse(BaseModel, Generic[T])`.

---

## §2 — `feedback/errors.py` — structured error codes (Gap #1)

```python
# feedback/errors.py
from enum import Enum


class ErrorCode(str, Enum):
    # 4xx
    VALIDATION_ERROR     = "FB_VALIDATION_ERROR"      # 422
    MISSING_TOKEN        = "FB_AUTH_MISSING_TOKEN"    # 401
    INVALID_TOKEN        = "FB_AUTH_INVALID_TOKEN"    # 401
    FORBIDDEN            = "FB_AUTH_FORBIDDEN"         # 403
    NOT_FOUND            = "FB_NOT_FOUND"             # 404
    DUPLICATE_FEEDBACK   = "FB_DUPLICATE"             # 409
    INVALID_STATUS       = "FB_INVALID_STATUS"        # 422
    # 5xx
    DB_ERROR             = "FB_DB_ERROR"              # 500
    INTERNAL_ERROR       = "FB_INTERNAL_ERROR"        # 500


class AppException(Exception):
    """Raised anywhere in the service; converted to an envelope by the handler in main.py."""
    def __init__(self, code: ErrorCode, message: str, http_status: int, detail: str | None = None):
        self.code = code
        self.message = message
        self.http_status = http_status
        self.detail = detail
        super().__init__(message)


# Convenience constructors
def not_found(entity: str = "Feedback record") -> AppException:
    return AppException(ErrorCode.NOT_FOUND, f"{entity} not found", 404)

def duplicate_feedback() -> AppException:
    return AppException(
        ErrorCode.DUPLICATE_FEEDBACK,
        "Feedback for this chat event has already been submitted by this user",
        409,
    )

def db_error(detail: str | None = None) -> AppException:
    return AppException(ErrorCode.DB_ERROR, "Failed to process feedback", 500, detail)
```

---

## §3 — `feedback/schemas.py` — new columns + duplicate constraint (Gaps #4, #6)

```python
# feedback/schemas.py
import uuid

from sqlalchemy import (
    Boolean, Column, DateTime, Float, JSON, String, Text,
    UniqueConstraint, func,
)
from sqlalchemy.dialects.postgresql import UUID

from feedback.db import Base


class UserFeedback(Base):
    __tablename__ = "user_feedback"

    # ── existing columns — types match the live data model ──
    feedback_id       = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    soe_id            = Column(String(64),  nullable=False)        # SOEID — returned by GET (per requirement)
    correlation_id    = Column(String(64),  nullable=False, index=True)  # MANDATORY — unique chat-event id
    trace_id          = Column(UUID(as_uuid=True), nullable=False) # GenAI call trace (may be 1 per chat event)
    usecase_id        = Column(String(64),  nullable=False)
    prompt            = Column(Text,        nullable=False)        # PII — never returned by GET
    metadata          = Column(JSON,        nullable=True)         # RAG sources
    response_text     = Column(JSON,        nullable=False)
    feedback_type     = Column(String(128), nullable=False)
    confidence_score  = Column(Float,       nullable=True)
    correctness       = Column(Boolean,     nullable=True)
    comments          = Column(Text,        nullable=True)         # PII — never returned by GET

    # ── NEW ──
    status            = Column(String(32),  nullable=False, server_default="open")
    created_at        = Column(DateTime(timezone=True), nullable=False, server_default=func.now())
    updated_at        = Column(DateTime(timezone=True), nullable=False,
                               server_default=func.now(), onupdate=func.now())

    # ── duplicate detection: one feedback per (user, chat event) ──
    # correlation_id identifies the unique chat event, so it is the natural dedup key
    # (was trace_id; switched because correlation_id is the chat-event identifier).
    __table_args__ = (
        UniqueConstraint("soe_id", "correlation_id", name="uq_feedback_soe_corr"),
    )
```

> **`correlation_id` — the mandatory chat-event key.** It identifies every unique chat event
> and is the primary join key for linking feedback back to the conversation. It lives on two
> surfaces:
> - **Business field** (this column + request body) — required, validated, persisted, returned by GET.
> - **Request-tracing** — the `X-Correlation-ID` header captured by `asgi-correlation-id`,
>   which appears in the response envelope and every log line.
>
> The caller **must** send the chat-event `correlation_id` in the request body, and **should**
> also set the `X-Correlation-ID` header to the same value so the stored field, the logs, and
> the envelope all line up end-to-end.

---

## §4 — `feedback/metrics.py` — Prometheus counters (Gap #7)

```python
# feedback/metrics.py
from prometheus_client import Counter, Histogram

FEEDBACK_SUBMITTED = Counter(
    "feedback_submitted_total",
    "Total feedback records persisted",
    ["usecase_id", "feedback_type", "correctness"],
)
FEEDBACK_DUPLICATE = Counter(
    "feedback_duplicate_total",
    "Duplicate feedback submissions rejected",
    ["usecase_id"],
)
FEEDBACK_ERRORS = Counter(
    "feedback_errors_total",
    "Errors by code",
    ["error_code"],
)
AUTH_FAILURES = Counter(
    "feedback_auth_failures_total",
    "Auth failures by reason",
    ["error_code"],
)
DB_LATENCY = Histogram(
    "feedback_db_operation_seconds",
    "DB operation latency",
    ["operation"],   # create | list | summary | get | update
)
```

---

## §5 — `feedback/models.py` — read-side Pydantic models

**First, make `correlation_id` mandatory on the inbound request model:**
```python
# feedback/models.py — on the EXISTING request model UserFeedbackModel
from pydantic import BaseModel, Field

class UserFeedbackModel(BaseModel):
    correlation_id:  str = Field(..., min_length=1)   # MANDATORY — unique chat-event id
    soe_id:          str
    trace_id:        str
    usecase_id:      str
    # ... existing fields: prompt, response_text, feedback_type, metadata,
    #     confidence_score, correctness, comments ...
```

**Then the read-side models:**
```python
# feedback/models.py  (append — keep existing UserFeedbackModel etc.)
from datetime import datetime
from typing import Optional
from pydantic import BaseModel


class FeedbackRecord(BaseModel):
    """Read projection. Returns soe_id (per requirement). Still excludes prompt,
    comments, response_text — those remain too sensitive/bulky for list views."""
    feedback_id:      str
    correlation_id:   str            # mandatory chat-event id
    trace_id:         str
    usecase_id:       str
    soe_id:           str            # SOEID returned per requirement
    feedback_type:    str
    confidence_score: Optional[float]
    correctness:      Optional[bool]
    status:           str
    has_comments:     bool
    rag_source_count: int
    created_at:       datetime
    updated_at:       datetime

    class Config:
        from_attributes = True


class FeedbackListData(BaseModel):
    items:     list[FeedbackRecord]
    total:     int
    page:      int
    page_size: int
    has_next:  bool


class DailyFeedbackPoint(BaseModel):
    date:             str
    total:            int
    correct_count:    int
    incorrect_count:  int
    no_correctness:   int
    correctness_rate: Optional[float]
    avg_confidence:   Optional[float]


class FeedbackSummaryData(BaseModel):
    total_count:             int
    correctness_rate:        Optional[float]
    avg_confidence_score:    Optional[float]
    thumbs_up_count:         int
    thumbs_down_count:       int
    open_count:              int
    reviewed_count:          int
    fixed_count:             int
    feedback_type_breakdown: dict[str, int]
    trends:                  list[DailyFeedbackPoint]


class ProblemRecord(BaseModel):
    """Detailed view of a reported problem (CAAR /getReportProblemDetails parity)."""
    feedback_id:      str
    correlation_id:   str            # mandatory chat-event id
    trace_id:         str
    usecase_id:       str
    soe_id:           str            # SOEID returned per requirement
    feedback_type:    str
    confidence_score: Optional[float]
    status:           str
    rag_source_count: int
    created_at:       datetime


class FeedbackStatusUpdate(BaseModel):
    from pydantic import Field
    status: str = Field(..., pattern="^(open|reviewed|fixed)$")
```

---

## §6 — `feedback/repositories.py` — read methods + duplicate detection + audit (Gaps #3, #6, #8)

```python
# feedback/repositories.py
import logging
from datetime import date
from typing import Optional

from sqlalchemy import Date, Integer, cast, func
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session

from feedback.errors import db_error, duplicate_feedback
from feedback.metrics import FEEDBACK_DUPLICATE, FEEDBACK_SUBMITTED, DB_LATENCY
from feedback.schemas import UserFeedback

log = logging.getLogger(__name__)
audit = logging.getLogger("audit")          # dedicated audit logger (Gap #8)


class UserFeedbackRepo:

    # ── CREATE (now with dup detection + audit) ──
    @staticmethod
    def create(db: Session, fb) -> UserFeedback:
        record = UserFeedback(
            correlation_id=fb.correlation_id,
            soe_id=fb.soe_id,
            trace_id=fb.trace_id,
            usecase_id=fb.usecase_id,
            prompt=fb.prompt,
            metadata=fb.metadata,
            response_text=fb.response_text,
            feedback_type=fb.feedback_type,
            confidence_score=fb.confidence_score,
            correctness=fb.correctness,
            comments=fb.comments,
            status="open",
        )
        try:
            with DB_LATENCY.labels(operation="create").time():
                db.add(record)
                db.commit()
                db.refresh(record)
        except IntegrityError:
            db.rollback()
            FEEDBACK_DUPLICATE.labels(usecase_id=fb.usecase_id).inc()
            audit.warning("feedback_duplicate_rejected", extra={
                "event_type": "feedback_duplicate", "correlation_id": fb.correlation_id,
                "trace_id": str(fb.trace_id),
                "soe_id": fb.soe_id, "usecase_id": fb.usecase_id,
                "component": "repositories",
            })
            raise duplicate_feedback()
        except Exception as exc:
            db.rollback()
            log.error("feedback_db_error", extra={
                "error_code": "FB_DB_ERROR", "correlation_id": fb.correlation_id,
                "trace_id": str(fb.trace_id),
                "soe_id": fb.soe_id, "component": "repositories",
            }, exc_info=True)
            raise db_error(type(exc).__name__)

        # metrics + audit on success
        FEEDBACK_SUBMITTED.labels(
            usecase_id=fb.usecase_id,
            feedback_type=fb.feedback_type,
            correctness=str(fb.correctness),
        ).inc()
        audit.info("feedback_submitted", extra={
            "event_type": "feedback_submitted",
            "feedback_id": str(record.feedback_id),
            "correlation_id": record.correlation_id,
            "trace_id": str(record.trace_id),
            "usecase_id": record.usecase_id,
            "feedback_type": record.feedback_type,
            "correctness": record.correctness,
            "soe_id": record.soe_id,
            "component": "repositories",
        })
        return record

    # ── READ: single ──
    @staticmethod
    def get_by_id(db: Session, feedback_id: str) -> Optional[UserFeedback]:
        with DB_LATENCY.labels(operation="get").time():
            return db.query(UserFeedback).filter(
                UserFeedback.feedback_id == feedback_id
            ).first()

    # ── READ: paginated list ──
    @staticmethod
    def list_feedback(db, usecase_id=None, correlation_id=None, feedback_type=None,
                      correctness=None, status=None, from_date=None, to_date=None,
                      page=1, page_size=50):
        q = db.query(UserFeedback)
        if usecase_id: q = q.filter(UserFeedback.usecase_id == usecase_id)
        if correlation_id: q = q.filter(UserFeedback.correlation_id == correlation_id)
        if feedback_type:  q = q.filter(UserFeedback.feedback_type == feedback_type)
        if correctness is not None: q = q.filter(UserFeedback.correctness == correctness)
        if status:         q = q.filter(UserFeedback.status == status)
        if from_date:      q = q.filter(cast(UserFeedback.created_at, Date) >= from_date)
        if to_date:        q = q.filter(cast(UserFeedback.created_at, Date) <= to_date)
        with DB_LATENCY.labels(operation="list").time():
            total = q.count()
            items = (q.order_by(UserFeedback.created_at.desc())
                      .offset((page - 1) * page_size).limit(page_size).all())
        return items, total

    # ── READ: summary (CAAR /getFeedBackSummary parity) ──
    @staticmethod
    def get_summary(db, usecase_id=None, from_date=None, to_date=None) -> dict:
        q = db.query(UserFeedback)
        if usecase_id: q = q.filter(UserFeedback.usecase_id == usecase_id)
        if from_date:      q = q.filter(cast(UserFeedback.created_at, Date) >= from_date)
        if to_date:        q = q.filter(cast(UserFeedback.created_at, Date) <= to_date)

        with DB_LATENCY.labels(operation="summary").time():
            total       = q.count()
            thumbs_up   = q.filter(UserFeedback.correctness.is_(True)).count()
            thumbs_down = q.filter(UserFeedback.correctness.is_(False)).count()
            avg_conf    = q.with_entities(func.avg(UserFeedback.confidence_score)).scalar()

            status_rows = (q.with_entities(UserFeedback.status, func.count())
                            .group_by(UserFeedback.status).all())
            type_rows   = (q.with_entities(UserFeedback.feedback_type, func.count())
                            .group_by(UserFeedback.feedback_type).all())
            daily_rows  = (q.with_entities(
                                cast(UserFeedback.created_at, Date).label("day"),
                                func.count().label("total"),
                                func.sum(func.cast(UserFeedback.correctness.is_(True), Integer)).label("correct"),
                                func.sum(func.cast(UserFeedback.correctness.is_(False), Integer)).label("incorrect"),
                                func.avg(UserFeedback.confidence_score).label("avg_conf"))
                            .group_by("day").order_by("day").all())

        status_map = {s: c for s, c in status_rows}
        voted = thumbs_up + thumbs_down
        daily = []
        for r in daily_rows:
            v = (r.correct or 0) + (r.incorrect or 0)
            daily.append({
                "date": str(r.day), "total": r.total,
                "correct_count": r.correct or 0, "incorrect_count": r.incorrect or 0,
                "no_correctness": r.total - v,
                "correctness_rate": round(r.correct / v, 4) if v else None,
                "avg_confidence": round(float(r.avg_conf), 4) if r.avg_conf else None,
            })
        return {
            "total_count": total,
            "correctness_rate": round(thumbs_up / voted, 4) if voted else None,
            "avg_confidence_score": round(float(avg_conf), 4) if avg_conf else None,
            "thumbs_up_count": thumbs_up, "thumbs_down_count": thumbs_down,
            "open_count": status_map.get("open", 0),
            "reviewed_count": status_map.get("reviewed", 0),
            "fixed_count": status_map.get("fixed", 0),
            "feedback_type_breakdown": {t: c for t, c in type_rows},
            "trends": daily,
        }

    # ── READ: problem details (CAAR /getReportProblemDetails parity) ──
    @staticmethod
    def list_problems(db, usecase_id=None, from_date=None, to_date=None,
                      page=1, page_size=50):
        """Negative / problem feedback only: correctness == False."""
        q = db.query(UserFeedback).filter(UserFeedback.correctness.is_(False))
        if usecase_id: q = q.filter(UserFeedback.usecase_id == usecase_id)
        if from_date:      q = q.filter(cast(UserFeedback.created_at, Date) >= from_date)
        if to_date:        q = q.filter(cast(UserFeedback.created_at, Date) <= to_date)
        total = q.count()
        items = (q.order_by(UserFeedback.created_at.desc())
                  .offset((page - 1) * page_size).limit(page_size).all())
        return items, total

    # ── UPDATE: status lifecycle ──
    @staticmethod
    def update_status(db, feedback_id, new_status) -> Optional[UserFeedback]:
        record = db.query(UserFeedback).filter(
            UserFeedback.feedback_id == feedback_id).first()
        if not record:
            return None
        old = record.status
        record.status = new_status
        db.commit(); db.refresh(record)
        audit.info("feedback_status_updated", extra={
            "event_type": "feedback_status_updated",
            "feedback_id": feedback_id, "old_status": old, "new_status": new_status,
            "component": "repositories",
        })
        return record
```

---

## §7 — `feedback/api/v1/api.py` — all routes wrapped in envelope

```python
# feedback/api/v1/api.py
import logging
from datetime import date
from typing import Optional

from asgi_correlation_id import correlation_id as corr_id_ctx
from fastapi import APIRouter, Depends, Query
from sqlalchemy.orm import Session

from feedback.auth import JWTBearer
from feedback.db import get_db
from feedback.errors import not_found
from feedback.models import (
    FeedbackListData, FeedbackRecord, FeedbackSummaryData,
    FeedbackStatusUpdate, ProblemRecord, UserFeedbackModel,
)
from feedback.repositories import UserFeedbackRepo
from feedback.responses import ApiResponse, success

router = APIRouter()
log = logging.getLogger(__name__)


def _cid() -> str:
    return corr_id_ctx.get() or "-"


def _to_record(r) -> FeedbackRecord:
    return FeedbackRecord(
        feedback_id=str(r.feedback_id), correlation_id=r.correlation_id,
        trace_id=str(r.trace_id),
        usecase_id=r.usecase_id, soe_id=r.soe_id, feedback_type=r.feedback_type,
        confidence_score=r.confidence_score, correctness=r.correctness,
        status=r.status, has_comments=bool(r.comments),
        rag_source_count=len(r.metadata) if r.metadata else 0,
        created_at=r.created_at, updated_at=r.updated_at,
    )


# ── POST (existing, now envelope + 201) ──
@router.post("/feedback", response_model=ApiResponse[dict], status_code=201)
async def submit_feedback(body: UserFeedbackModel,
                          db: Session = Depends(get_db),
                          _t: str = Depends(JWTBearer())):
    record = UserFeedbackRepo.create(db, body)   # raises AppException on dup/db error
    return success(
        {"feedback_id": str(record.feedback_id), "correlation_id": record.correlation_id},
        _cid())


# ── GET list ──
@router.get("/feedback", response_model=ApiResponse[FeedbackListData])
async def list_feedback(
    usecase_id:     Optional[str]  = Query(None),
    correlation_id: Optional[str]  = Query(None, description="All feedback for one chat event"),
    feedback_type:  Optional[str]  = Query(None),
    correctness:    Optional[bool] = Query(None),
    status:         Optional[str]  = Query(None),
    from_date:      Optional[date] = Query(None),
    to_date:        Optional[date] = Query(None),
    page:           int            = Query(1, ge=1),
    page_size:      int            = Query(50, ge=1, le=200),
    db: Session = Depends(get_db), _t: str = Depends(JWTBearer()),
):
    items, total = UserFeedbackRepo.list_feedback(
        db, usecase_id, correlation_id, feedback_type, correctness, status,
        from_date, to_date, page, page_size)
    data = FeedbackListData(
        items=[_to_record(r) for r in items], total=total,
        page=page, page_size=page_size, has_next=(page * page_size) < total)
    return success(data, _cid())


# ── GET summary  (CAAR /getFeedBackSummary) ──
@router.get("/feedback/summary", response_model=ApiResponse[FeedbackSummaryData])
async def feedback_summary(
    usecase_id: Optional[str]  = Query(None),
    from_date:      Optional[date] = Query(None),
    to_date:        Optional[date] = Query(None),
    db: Session = Depends(get_db), _t: str = Depends(JWTBearer()),
):
    data = UserFeedbackRepo.get_summary(db, usecase_id, from_date, to_date)
    return success(FeedbackSummaryData(**data), _cid())


# ── GET problems  (CAAR /getReportProblemDetails) ──
@router.get("/feedback/problems", response_model=ApiResponse[list[ProblemRecord]])
async def feedback_problems(
    usecase_id: Optional[str]  = Query(None),
    from_date:      Optional[date] = Query(None),
    to_date:        Optional[date] = Query(None),
    page:           int            = Query(1, ge=1),
    page_size:      int            = Query(50, ge=1, le=200),
    db: Session = Depends(get_db), _t: str = Depends(JWTBearer()),
):
    items, total = UserFeedbackRepo.list_problems(
        db, usecase_id, from_date, to_date, page, page_size)
    data = [ProblemRecord(
        feedback_id=str(r.feedback_id), correlation_id=r.correlation_id,
        trace_id=str(r.trace_id),
        usecase_id=r.usecase_id, soe_id=r.soe_id, feedback_type=r.feedback_type,
        confidence_score=r.confidence_score, status=r.status,
        rag_source_count=len(r.metadata) if r.metadata else 0,
        created_at=r.created_at) for r in items]
    return success(data, _cid())


# ── GET by id ──
@router.get("/feedback/{feedback_id}", response_model=ApiResponse[FeedbackRecord])
async def get_feedback(feedback_id: str,
                       db: Session = Depends(get_db), _t: str = Depends(JWTBearer())):
    record = UserFeedbackRepo.get_by_id(db, feedback_id)
    if not record:
        raise not_found()
    return success(_to_record(record), _cid())


# ── PATCH status ──
@router.patch("/feedback/{feedback_id}/status", response_model=ApiResponse[FeedbackRecord])
async def update_status(feedback_id: str, body: FeedbackStatusUpdate,
                        db: Session = Depends(get_db), _t: str = Depends(JWTBearer())):
    record = UserFeedbackRepo.update_status(db, feedback_id, body.status)
    if not record:
        raise not_found()
    return success(_to_record(record), _cid())
```

> **Route order matters in FastAPI:** declare `/feedback/summary` and `/feedback/problems`
> **before** `/feedback/{feedback_id}`, otherwise "summary"/"problems" get captured as an ID.

---

## §8 — `feedback/main.py` — exception handlers, /metrics, PII redaction (Gaps #1, #5, #7, #10)

```python
# feedback/main.py  (additions)
import json, logging, os, time
from asgi_correlation_id import correlation_id as corr_id_ctx
from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse
from prometheus_client import CONTENT_TYPE_LATEST, generate_latest
from starlette.responses import Response

from feedback.errors import AppException, ErrorCode
from feedback.metrics import AUTH_FAILURES, FEEDBACK_ERRORS
from feedback.responses import failure

log = logging.getLogger(__name__)
# soe_id is intentionally NOT redacted (retained per requirement). prompt / comments /
# response_text are still masked — they can carry free-text business-sensitive content.
_REDACT = {"prompt", "comments", "response_text"}
_PROD = os.environ.get("DEPLOYMENT_ENVIRONMENT", "dev").lower() in {"prod", "production"}


# ── 1. Convert AppException → uniform error envelope (Gap #1, #2) ──
@app.exception_handler(AppException)
async def app_exception_handler(request: Request, exc: AppException):
    FEEDBACK_ERRORS.labels(error_code=exc.code.value).inc()
    return JSONResponse(
        status_code=exc.http_status,
        content=failure(exc.code.value, exc.message, corr_id_ctx.get() or "-", exc.detail),
    )

# ── 2. Catch-all so raw exceptions never leak (Gap #1) ──
@app.exception_handler(Exception)
async def unhandled_handler(request: Request, exc: Exception):
    log.error("unhandled_exception", extra={
        "error_code": "FB_INTERNAL_ERROR", "path": request.url.path,
        "exc_type": type(exc).__name__, "component": "main"}, exc_info=True)
    FEEDBACK_ERRORS.labels(error_code="FB_INTERNAL_ERROR").inc()
    return JSONResponse(status_code=500, content=failure(
        ErrorCode.INTERNAL_ERROR.value, "Internal server error", corr_id_ctx.get() or "-"))


# ── 3. Prometheus metrics endpoint (Gap #7) ──
@app.get("/metrics", include_in_schema=False)
async def metrics():
    return Response(generate_latest(), media_type=CONTENT_TYPE_LATEST)


# ── 4. PII-safe structured request logging (Gaps #5, #10) ──
def _redact(body: dict) -> dict:
    # soe_id passes through unchanged (retained per requirement).
    return {k: ("<redacted>" if k in _REDACT else v) for k, v in body.items()}

@app.middleware("http")
async def calculate_process_time(request: Request, call_next):
    start = time.perf_counter()
    response = await call_next(request)
    latency_ms = round((time.perf_counter() - start) * 1000, 2)

    log.log(
        logging.WARNING if latency_ms > float(
            os.environ.get("RESPONSE_WARNING_THRESHOLD_SECONDS", "30")) * 1000
        else logging.INFO,
        "http_request",
        extra={                                  # Gap #10: structured, not f-string
            "method": request.method,
            "path": request.url.path,
            "status_code": response.status_code,  # Gap: status was missing
            "latency_ms": latency_ms,             # Gap: was seconds-as-string
            "slow_response": latency_ms > float(
                os.environ.get("RESPONSE_WARNING_THRESHOLD_SECONDS", "30")) * 1000,
            "component": "middleware",
        },
    )
    response.headers["X-Process-Time-Ms"] = str(latency_ms)
    return response
```

> The middleware no longer logs raw request/response bodies. If you need body capture for
> debugging in DEV only, gate it behind `if not _PROD:` and pass it through `_redact()`.

**`feedback/auth.py`** — log + count auth failures with a structured code (Gap #8):
```python
import logging
log = logging.getLogger(__name__)
audit = logging.getLogger("audit")
# inside JWTBearer.__call__, on every rejection:
audit.warning("auth_failed", extra={
    "event_type": "auth_failure", "error_code": "FB_AUTH_INVALID_TOKEN",
    "path": str(request.url.path), "component": "auth"})
from feedback.metrics import AUTH_FAILURES
AUTH_FAILURES.labels(error_code="FB_AUTH_INVALID_TOKEN").inc()
raise AppException(ErrorCode.INVALID_TOKEN, "Unauthorized", 401)
```

---

## §9 — logging: `AppInfoFilter` + `logconfig.yaml` (E-P0-1, E-P1-5, E-P1-8, E-P1-10)

### `feedback/log_filters.py` — guarantee static + context fields on every record

`environment` and `service_name` come from env vars (not per-request `extra=`), so a filter
must stamp them on **every** record. The filter also supplies safe defaults (`"-"`) so the
JSON formatter never emits `null` for `trace_id` / `feedback_id` when they're absent.

```python
# feedback/log_filters.py
import logging
import os

# NOTE: rename the existing ContextVar in feedback/tracking.py from
# `application_id_var` to `usecase_id_var`. It is sourced from the request header,
# which is ALSO renamed: X-Application-ID -> X-Usecase-ID. Update the middleware that
# reads the header (it must now read `request.headers.get("X-Usecase-ID")`).
from feedback.tracking import usecase_id_var


class AppInfoFilter(logging.Filter):
    _environment  = os.environ.get("DEPLOYMENT_ENVIRONMENT", "unknown")   # E-P1-8
    _service_name = os.environ.get("APPLICATION_NAME", "genai-user-feedback")

    def filter(self, record: logging.LogRecord) -> bool:
        record.environment  = self._environment
        record.service_name = self._service_name
        record.usecase_id = getattr(record, "usecase_id", usecase_id_var.get("-"))
        # defaults so formatter never crashes / never prints null.
        # correlation_id default is set here; the asgi-correlation-id filter (which runs
        # after this one) overwrites it with the real X-Correlation-ID when present.
        for f in ("correlation_id", "trace_id", "feedback_id", "soe_id", "event_type",
                  "error_code", "component", "status_code", "latency_ms"):
            if not hasattr(record, f):
                setattr(record, f, "-")
        return True
```

### `logconfig.yaml` — wire the filter + `audit` logger

```yaml
filters:
  app_info:
    (): feedback.log_filters.AppInfoFilter

handlers:
  json_console:
    class: logging.StreamHandler
    formatter: json
    filters: [app_info, correlation_id]     # correlation_id from asgi-correlation-id

loggers:
  audit:                                     # E-P1-10 — tamper-evident stream
    level: INFO
    handlers: [json_console]
    propagate: false                         # audit stays distinct from app logs

formatters:
  json:
    (): pythonjsonlogger.jsonlogger.JsonFormatter
    format: >-
      %(asctime)s %(name)s %(levelname)s %(message)s
      %(environment)s %(service_name)s %(event_type)s %(component)s
      %(error_code)s %(status_code)s %(latency_ms)s
      %(correlation_id)s %(usecase_id)s %(trace_id)s %(feedback_id)s %(soe_id)s
    rename_fields: {asctime: time, levelname: level}
```

Audit events (`event_type` = `feedback_submitted` / `feedback_duplicate` /
`feedback_status_updated` / `auth_failure`) are emitted on logger `audit`, so Fluent Bit can
route them to a dedicated Elasticsearch index (`ai-obs-feedback-audit-*`) separate from app logs.

---

## §10 — `alembic/versions/002_feedback_readiness.py`

```python
"""correlation_id, status, timestamps, unique(soe_id,correlation_id), indexes"""
from alembic import op
import sqlalchemy as sa

def upgrade():
    op.add_column("user_feedback", sa.Column("status", sa.String(32),
                  nullable=False, server_default="open"))
    op.add_column("user_feedback", sa.Column("created_at", sa.DateTime(timezone=True),
                  nullable=False, server_default=sa.func.now()))
    op.add_column("user_feedback", sa.Column("updated_at", sa.DateTime(timezone=True),
                  nullable=False, server_default=sa.func.now()))

    # ── correlation_id: mandatory NOT NULL. Add nullable first, backfill legacy rows
    #    (no real chat-event id exists for them → fall back to trace_id), then enforce. ──
    op.add_column("user_feedback", sa.Column("correlation_id", sa.String(64), nullable=True))
    op.execute("UPDATE user_feedback SET correlation_id = trace_id::text "
               "WHERE correlation_id IS NULL;")
    op.alter_column("user_feedback", "correlation_id", nullable=False)

    # de-dup existing rows BEFORE adding the unique constraint.
    # dedup key is (soe_id, correlation_id) — one feedback per user per chat event.
    op.execute("""
        DELETE FROM user_feedback a USING user_feedback b
        WHERE a.ctid < b.ctid AND a.soe_id = b.soe_id
          AND a.correlation_id = b.correlation_id;
    """)
    op.create_unique_constraint("uq_feedback_soe_corr", "user_feedback",
                                ["soe_id", "correlation_id"])

    op.create_index("ix_uf_correlation_id", "user_feedback", ["correlation_id"])
    op.create_index("ix_uf_usecase_id", "user_feedback", ["usecase_id"])
    op.create_index("ix_uf_created_at", "user_feedback", ["created_at"])
    op.create_index("ix_uf_status", "user_feedback", ["status"])
    op.create_index("ix_uf_correctness", "user_feedback", ["correctness"])
    op.create_index("ix_uf_app_created", "user_feedback", ["usecase_id", "created_at"])

def downgrade():
    for ix in ["ix_uf_app_created","ix_uf_correctness","ix_uf_status",
               "ix_uf_created_at","ix_uf_usecase_id","ix_uf_correlation_id"]:
        op.drop_index(ix, "user_feedback")
    op.drop_constraint("uq_feedback_soe_corr", "user_feedback", type_="unique")
    for col in ["correlation_id","updated_at","created_at","status"]:
        op.drop_column("user_feedback", col)
```

---

## API reference — endpoint → CAAR parity → dashboard use

| Method | Endpoint | CAAR equivalent | Dashboard use |
|---|---|---|---|
| `POST` | `/api/v1/feedback` | submit | (existing — now dedup + envelope) |
| `GET` | `/api/v1/feedback/summary` | `/getFeedBackSummary` | Overview cards + trend chart |
| `GET` | `/api/v1/feedback/problems` | `/getReportProblemDetails` | "Reported problems" table |
| `GET` | `/api/v1/feedback` | — | Filterable recent-feedback table |
| `GET` | `/api/v1/feedback/{id}` | — | Detail drawer |
| `PATCH` | `/api/v1/feedback/{id}/status` | — | "Mark reviewed / fixed" |
| `GET` | `/metrics` | Actuator/Prometheus | Custom Dashboard Kafka/health panel scrapes |

### Request contract (POST `/api/v1/feedback`)

**Headers**

| Header | Required | Notes |
|---|---|---|
| `Authorization: Bearer <COIN JWT>` | Yes | Validated on every route |
| `X-Usecase-ID` | Yes | CSI id of the consuming use case (was `X-Application-ID`) |
| `X-Correlation-ID` | Yes | Chat-event id — set to the same value as the body's `correlation_id` |

**Body (required fields shown)**

```json
{
  "correlation_id": "chat-evt-8f3a...",   // MANDATORY — unique chat event
  "soe_id": "ab12345",
  "trace_id": "uuid-of-genai-call",
  "usecase_id": "CSI-179443",
  "prompt": "…",
  "response_text": { "...": "..." },
  "feedback_type": "incorrect_response",
  "metadata": [ { "source": "…", "document_id": "…" } ],
  "confidence_score": 0.42,
  "correctness": false,
  "comments": "…"
}
```

A request missing `correlation_id` is rejected with `422` / `FB_VALIDATION_ERROR`.

### Sample envelope responses

Success (POST):
```json
{
  "status": "success",
  "data": { "feedback_id": "f7c1...", "correlation_id": "chat-evt-8f3a..." },
  "error": null,
  "correlation_id": "chat-evt-8f3a...",
  "timestamp": "2026-06-02T10:45:12.345Z"
}
```

Single record (GET `/api/v1/feedback/{id}`):
```json
{
  "status": "success",
  "data": {
    "feedback_id": "f7c1...",
    "correlation_id": "chat-evt-8f3a...",
    "trace_id": "uuid-of-genai-call",
    "usecase_id": "CSI-179443",
    "soe_id": "ab12345",
    "feedback_type": "incorrect_response",
    "confidence_score": 0.42,
    "correctness": false,
    "status": "open",
    "has_comments": true,
    "rag_source_count": 3,
    "created_at": "2026-06-02T10:45:12Z",
    "updated_at": "2026-06-02T10:45:12Z"
  },
  "error": null,
  "correlation_id": "chat-evt-8f3a...",
  "timestamp": "2026-06-02T10:45:12.345Z"
}
```

Duplicate (HTTP 409):
```json
{
  "status": "error",
  "data": null,
  "error": {
    "code": "FB_DUPLICATE",
    "message": "Feedback for this chat event has already been submitted by this user",
    "detail": null
  },
  "correlation_id": "chat-evt-8f3a...",
  "timestamp": "2026-06-02T10:45:12.345Z"
}
```

---

## §11 — `feedback_type` enum constraint (E-P1-1)

Free-text `feedback_type` fragments analytics (`"wrong"`, `"Wrong"`, `"incorrect_response"`
all count separately). Constrain it to a controlled vocabulary — but roll out **permissively**
first so you don't reject in-flight clients.

```python
# feedback/models.py
from enum import Enum

class FeedbackType(str, Enum):
    INCORRECT_RESPONSE = "incorrect_response"
    INCOMPLETE         = "incomplete"
    LOW_CONFIDENCE     = "low_confidence"
    SLOW               = "slow"
    TOOL_FAILED        = "tool_failed"
    UNSAFE             = "unsafe"
    RAG_MISSING        = "rag_missing"
    FORMATTING         = "formatting"
    OTHER              = "other"
```

**Phased enforcement (avoids the "Medium risk — rejects previously valid requests"):**

```python
# feedback/api/v1/api.py — inside submit_feedback, before repo.create
import os
_STRICT = os.environ.get("FEEDBACK_TYPE_STRICT", "false").lower() == "true"

if body.feedback_type not in {e.value for e in FeedbackType}:
    if _STRICT:
        raise AppException(ErrorCode.VALIDATION_ERROR,
                           f"Invalid feedback_type '{body.feedback_type}'", 422)
    # permissive mode: accept but log + count for migration tracking
    log.warning("feedback_type_unknown", extra={
        "feedback_type": body.feedback_type, "component": "api"})
    FEEDBACK_ERRORS.labels(error_code="FB_FEEDBACK_TYPE_UNKNOWN").inc()
    body.feedback_type = FeedbackType.OTHER.value
```

> Run permissive (`FEEDBACK_TYPE_STRICT=false`) for ~2 weeks, watch the
> `feedback_type_unknown` log/metric to map legacy values, then flip to strict.

---

## §12 — `GET /feedback/by-type` analytics endpoint (E-P2-1)

The dashboard "feedback by category" bar chart wants a flat per-type rollup. `/summary`
already returns `feedback_type_breakdown`, but `/by-type` adds correctness split per type.

```python
# feedback/repositories.py
@staticmethod
def by_type(db, usecase_id=None, from_date=None, to_date=None) -> list[dict]:
    from sqlalchemy import Date, Integer, cast, func
    q = db.query(
        UserFeedback.feedback_type,
        func.count().label("total"),
        func.sum(func.cast(UserFeedback.correctness.is_(True), Integer)).label("correct"),
        func.sum(func.cast(UserFeedback.correctness.is_(False), Integer)).label("incorrect"),
        func.avg(UserFeedback.confidence_score).label("avg_conf"),
    )
    if usecase_id: q = q.filter(UserFeedback.usecase_id == usecase_id)
    if from_date:      q = q.filter(cast(UserFeedback.created_at, Date) >= from_date)
    if to_date:        q = q.filter(cast(UserFeedback.created_at, Date) <= to_date)
    rows = q.group_by(UserFeedback.feedback_type).order_by(func.count().desc()).all()
    return [{
        "feedback_type": r.feedback_type, "total": r.total,
        "correct_count": r.correct or 0, "incorrect_count": r.incorrect or 0,
        "avg_confidence": round(float(r.avg_conf), 4) if r.avg_conf else None,
    } for r in rows]
```

```python
# feedback/api/v1/api.py  (declare BEFORE /feedback/{feedback_id})
@router.get("/feedback/by-type", response_model=ApiResponse[list[dict]])
async def feedback_by_type(
    usecase_id: Optional[str]  = Query(None),
    from_date:      Optional[date] = Query(None),
    to_date:        Optional[date] = Query(None),
    db: Session = Depends(get_db), _t: str = Depends(JWTBearer()),
):
    return success(UserFeedbackRepo.by_type(db, usecase_id, from_date, to_date), _cid())
```

---

## §13 — optional 1–5 Likert `rating` field (E-P2-2)

Boolean `correctness` is coarse. Add an **optional** `rating` (1–5) — nullable so existing
clients are unaffected.

```python
# feedback/schemas.py — new column
rating = Column(Integer, nullable=True)   # 1..5, optional
```
```python
# feedback/models.py — on the inbound UserFeedbackModel
from pydantic import Field
rating: Optional[int] = Field(None, ge=1, le=5)
```
```python
# alembic — additive, no backfill needed
op.add_column("user_feedback", sa.Column("rating", sa.Integer(), nullable=True))
op.create_check_constraint("ck_uf_rating_range", "user_feedback",
                           "rating IS NULL OR (rating BETWEEN 1 AND 5)")
```
Expose `rating` on `FeedbackRecord` and add `avg_rating` to `FeedbackSummaryData` if the
dashboard wants a CSAT-style trend.

---

## §14 — OpenTelemetry span export (E-P2-3 — deferred)

Deferred until the platform OTel collector endpoint is confirmed. When ready it's a drop-in:

```python
# feedback/main.py
from opentelemetry.instrumentation.fastapi import FastAPIInstrumentor
from opentelemetry.instrumentation.sqlalchemy import SQLAlchemyInstrumentor
FastAPIInstrumentor.instrument_app(app)        # HTTP server spans
SQLAlchemyInstrumentor().instrument(engine=engine)   # DB spans
```
Set `OTEL_EXPORTER_OTLP_ENDPOINT` + `OTEL_SERVICE_NAME` via Helm. No code beyond this; spans
auto-correlate with the existing `X-Correlation-ID` via the `traceparent` header. Marked
**deferred** because it needs the collector address and adds a dependency the team hasn't
committed to yet.

---

## §15 — RBAC roles (E-P2-4 — High risk, gate carefully)

Today every authenticated caller can read and submit. The dashboard's review actions
(`PATCH /status`, list-all) should require an admin role; submit stays open to all callers.
**High risk** because it changes the auth model — ship behind a flag.

```python
# feedback/auth.py
class JWTBearer:
    def __init__(self, required_role: str | None = None):
        self.required_role = required_role
    async def __call__(self, request):
        payload = ...  # existing COIN verification
        if self.required_role and self.required_role not in payload.get("roles", []):
            audit.warning("rbac_denied", extra={
                "event_type": "rbac_denied", "required_role": self.required_role,
                "component": "auth"})
            raise AppException(ErrorCode.FORBIDDEN, "Insufficient role", 403)
        return payload
```
```python
# write/review routes require the reviewer role; submit + reads stay open
_ADMIN = JWTBearer(required_role=os.environ.get("FEEDBACK_ADMIN_ROLE", "feedback-admin"))

@router.patch("/feedback/{feedback_id}/status", ...)
async def update_status(..., _t = Depends(_ADMIN)): ...
```
> Roll out with `FEEDBACK_ADMIN_ROLE` unset (no enforcement) first; populate COIN roles,
> verify in UAT, then set the role name in PROD.

---

## §16 — OpenAPI contact / server metadata (E-P2-5)

Trivial DX improvement — no behaviour change.

```python
# feedback/main.py
app = FastAPI(
    title="GenAI User Feedback Service",
    version="2.0.0",
    description="Collects and serves user feedback on GenAI responses.",
    contact={"name": "GenAI Platform Team", "email": "genai-platform@citi.com"},
    servers=[
        {"url": "https://feedback.dev.apps.citi.net", "description": "DEV"},
        {"url": "https://feedback.uat.apps.citi.net", "description": "UAT"},
        {"url": "https://feedback.apps.citi.net",     "description": "PROD"},
    ],
    lifespan=lifespan,
)
```

---

## §17 — deletion / anonymisation endpoint (E-P2-7 — GDPR / retention)

Right to erasure: anonymise rather than hard-delete, to preserve aggregate analytics.

```python
# feedback/repositories.py
@staticmethod
def anonymise(db, feedback_id: str) -> Optional[UserFeedback]:
    r = db.query(UserFeedback).filter(UserFeedback.feedback_id == feedback_id).first()
    if not r:
        return None
    r.soe_id = "ANONYMISED"
    r.prompt = "<anonymised>"
    r.comments = None
    r.response_text = {"anonymised": True}
    db.commit(); db.refresh(r)
    audit.info("feedback_anonymised", extra={
        "event_type": "feedback_anonymised", "feedback_id": feedback_id,
        "component": "repositories"})
    return r
```
```python
# feedback/api/v1/api.py  — admin-only
@router.delete("/feedback/{feedback_id}", response_model=ApiResponse[dict])
async def anonymise_feedback(feedback_id: str,
                             db: Session = Depends(get_db), _t = Depends(_ADMIN)):
    r = UserFeedbackRepo.anonymise(db, feedback_id)
    if not r:
        raise not_found()
    return success({"feedback_id": feedback_id, "status": "anonymised"}, _cid())
```
Keeps the row (counts, ratings, type all intact) but strips every PII column. For a
hard-delete variant, swap the body for `db.delete(r)`.

---

## §18 — startup/shutdown events + route-level request logging

Two structured-logging niceties (no Kafka, no SDK — plain `logging`).

### 18a — lifespan startup / shutdown events

```python
# feedback/main.py
import logging, os
from contextlib import asynccontextmanager
from fastapi import FastAPI

log = logging.getLogger(__name__)

@asynccontextmanager
async def lifespan(app: FastAPI):
    # startup
    log.info("service_started", extra={
        "event_type": "service_started",
        "service_name": os.environ.get("APPLICATION_NAME", "genai-user-feedback"),
        "version": os.environ.get("SERVICE_VERSION", "unknown"),
        "environment": os.environ.get("DEPLOYMENT_ENVIRONMENT", "unknown"),
        "component": "lifespan",
    })
    yield
    # shutdown
    log.info("service_stopped", extra={
        "event_type": "service_stopped", "component": "lifespan"})

# already wired in §16: app = FastAPI(..., lifespan=lifespan)
```
> Add `SERVICE_VERSION` (e.g. the image tag / chart appVersion) to Helm `env:` so the
> startup line records the running build. `event_type` flows to the `audit`/app log stream
> via the §9 filter + formatter.

### 18b — route-level request logging in `api.py`

Repo-layer audit (§6) records the *persisted* event; this adds a thin route-layer
`received` / `completed` pair so a request is traceable even if it never reaches the DB
(e.g. validation rejects it). Wrap the existing POST handler:

```python
# feedback/api/v1/api.py
@router.post("/feedback", response_model=ApiResponse[dict], status_code=201)
async def submit_feedback(body: UserFeedbackModel,
                          db: Session = Depends(get_db),
                          _t: str = Depends(JWTBearer())):
    log.info("feedback_request_received", extra={
        "event_type": "feedback_request_received",
        "correlation_id": body.correlation_id,
        "usecase_id": body.usecase_id,
        "feedback_type": body.feedback_type,
        "component": "api",
    })
    record = UserFeedbackRepo.create(db, body)     # raises AppException on dup/db error
    log.info("feedback_request_completed", extra={
        "event_type": "feedback_request_completed",
        "feedback_id": str(record.feedback_id),
        "correlation_id": record.correlation_id,
        "status_code": 201,
        "component": "api",
    })
    return success(
        {"feedback_id": str(record.feedback_id), "correlation_id": record.correlation_id},
        _cid())
```
> Failures don't need a separate `try/except` here — the global handlers in §8 already log
> every `AppException` and unhandled exception with the correlation_id and error_code.

---

## requirements.txt additions

```
prometheus-client>=0.20.0
# opentelemetry-instrumentation-fastapi / -sqlalchemy   # only when E-P2-3 is scheduled
# (alembic already present; asgi-correlation-id already present)
```

No Langfuse, no obs-sdk, no Kafka — this service stays self-contained. The Custom Dashboard
reads everything it needs over the GET endpoints; audit events flow to Elasticsearch via the
existing Fluent Bit log pipeline.

---

## Build order (by backlog ID)

| Sprint | Backlog IDs | Sections | Effort | Notes |
|---|---|---|---|---|
| **P0-a** | E-P0-1, E-P0-2, E-P0-3, E-P0-6 | §6, §8, §9 | ~1 day | Pure-additive logging + PII redaction; ship first, zero client impact |
| **P0-b** | E-P0-5, E-P0-7 | §2, §8 | ~1 day | Error codes + global handler (stops traceback leaks) |
| **P0-c** | E-P0-4 | §1, §7 | ~1 day | Envelope — **breaking response shape**, coordinate with dashboard team |
| **P1-a** | E-P1-3, E-P1-4 | §3, §7, §10 | ~1.5 days | `status` column + GET retrieval API (unblocks dashboard reads) |
| **P1-b** | E-P1-2 | §3, §6 | ~0.5 day | Duplicate prevention (dedup existing rows in migration) |
| **P1-c** | E-P1-5…E-P1-8 | §8, §9 | ~0.5 day | trace_id / latency_ms / status_code / env / service_name in logs |
| **P1-d** | E-P1-9, E-P1-10 | §4, §8, §9 | ~1 day | Prometheus `/metrics` + audit logger |
| **P1-f** | (extras) | §18 | ~0.25 day | Startup/shutdown events + route-level request logging |
| **P1-e** | E-P1-1 | §11 | ~0.5 day | `feedback_type` enum — ship permissive, flip strict after 2 weeks |
| **P2** | E-P2-1, E-P2-2, E-P2-5, E-P2-7 | §12, §13, §16, §17 | ~2 days | by-type analytics, Likert rating, OpenAPI metadata, anonymisation |
| **P2-deferred** | E-P2-3, E-P2-4 | §14, §15 | — | OTel (needs collector), RBAC (High risk — gate). E-P2-6 `user_hash` dropped — raw `soe_id` retained |

**Total P0+P1 ≈ 7–8 engineering days.** P0 is mostly XS/S additive work; the only change
needing cross-team coordination is E-P0-4 (the response envelope).
