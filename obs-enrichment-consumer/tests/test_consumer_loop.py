"""Loop semantics with fake Kafka objects: routing, DLQ shape, commit-after-produce."""
import json

from obs_enrichment.consumer import EnrichmentLoop
from obs_enrichment.deps import Deps
from obs_enrichment.redactor import RegexRedactor
from tests.conftest import FakeArchiver, FakeProducer, make_raw


class FakeMsg:
    def __init__(self, value, key=b"corr-1", headers=None, error=None):
        self._value, self._key, self._headers, self._error = value, key, headers, error

    def value(self):
        return self._value

    def key(self):
        return self._key

    def headers(self):
        return self._headers

    def error(self):
        return self._error


class FakeConsumer:
    def __init__(self, batches):
        self._batches = list(batches)
        self.committed = 0
        self.closed = False

    def subscribe(self, topics):
        pass

    def consume(self, num_messages, timeout):
        return self._batches.pop(0) if self._batches else []

    def commit(self, asynchronous=False):
        self.committed += 1

    def close(self):
        self.closed = True


def _loop(settings, cp, batches, producer=None):
    deps = Deps(settings=settings, control_plane=cp, redactor=RegexRedactor(),
                archiver=FakeArchiver(), slo_tracker=None)
    producer = producer or FakeProducer()
    consumer = FakeConsumer(batches)
    loop = EnrichmentLoop(settings, deps, consumer, producer)

    # run exactly len(batches) iterations then stop
    n = len(batches)
    original_consume = consumer.consume

    def counting_consume(num_messages, timeout):
        nonlocal n
        if n == 0:
            loop.stop()
            return []
        n -= 1
        return original_consume(num_messages, timeout)

    consumer.consume = counting_consume
    loop.run()
    return consumer, producer


def test_good_events_produced_to_processed_and_committed(settings, cp):
    consumer, producer = _loop(settings, cp, [[FakeMsg(make_raw()), FakeMsg(make_raw())]])
    topics = [m["topic"] for m in producer.messages]
    assert topics == ["ai-obs-events-processed", "ai-obs-events-processed"]
    assert consumer.committed == 1
    assert consumer.closed
    out = json.loads(producer.messages[0]["value"])
    assert out["payload"]["app_owner_team"] == "obs-platform"


def test_bad_event_goes_to_dead_letter_with_replayable_shape(settings, cp):
    consumer, producer = _loop(settings, cp, [[FakeMsg(b"not json"), FakeMsg(make_raw())]])
    by_topic = {}
    for m in producer.messages:
        by_topic.setdefault(m["topic"], []).append(m)
    assert len(by_topic["ai-obs-dead-letter"]) == 1
    assert len(by_topic["ai-obs-events-processed"]) == 1
    wrapped = json.loads(by_topic["ai-obs-dead-letter"][0]["value"])
    assert set(wrapped) == {"reason", "failed_at", "original"}
    assert "stage1-validate" in wrapped["reason"]
    assert consumer.committed == 1  # DLQ-ing IS handling — batch commits


def test_delivery_failure_blocks_commit(settings, cp):
    producer = FakeProducer()
    producer.fail_next = 1
    consumer, _ = _loop(settings, cp, [[FakeMsg(make_raw())]], producer=producer)
    assert consumer.committed == 0  # batch will be redelivered
