"""Pipeline error types."""


class DeadLetterError(Exception):
    """Raised by a stage to quarantine the current event to ai-obs-dead-letter.

    Use for events that are *wrong* (unparseable, contract-violating).
    Transient infrastructure failures (DB down, S3 timeout) must NOT dead-letter
    an event — raise TransientError instead so the batch is retried.
    """

    def __init__(self, reason: str):
        super().__init__(reason)
        self.reason = reason


class TransientError(Exception):
    """Infrastructure hiccup — do not commit, do not dead-letter; retry the batch."""
