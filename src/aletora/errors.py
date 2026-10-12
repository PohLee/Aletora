"""Typed failures exposed by the Aletora domain interface."""


class AletoraError(Exception):
    """Base class for expected Aletora failures."""


class NotFound(AletoraError):
    """A scoped record does not exist or is not visible."""


class RevisionConflict(AletoraError):
    """A compare-and-swap write used a stale revision."""


class IdempotencyConflict(AletoraError):
    """An idempotency key was reused for different semantic content."""


class InvalidTransition(AletoraError):
    """A requested lifecycle transition is not permitted."""


class CapabilityUnavailable(AletoraError):
    """A required optional adapter capability is not configured."""
