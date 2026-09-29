"""Backend-neutral types shared by every backend.

Nothing in here may import a cloud SDK or pyspark at module level: these types are
constructed in the Metaflow task process, which is not guaranteed to have any of the
backend dependencies installed.
"""

import sys
from dataclasses import dataclass, field
from typing import Any, Callable, Dict, Optional


class JobState:
    PENDING = "PENDING"
    RUNNING = "RUNNING"
    SUCCESS = "SUCCESS"
    FAILED = "FAILED"
    CANCELLED = "CANCELLED"

    TERMINAL = frozenset({SUCCESS, FAILED, CANCELLED})

    @classmethod
    def is_terminal(cls, state):
        return state in cls.TERMINAL


@dataclass
class JobStatus:
    state: str
    message: Optional[str] = None
    error_class: Optional[str] = None
    ui_url: Optional[str] = None
    raw: Dict[str, Any] = field(default_factory=dict)

    @property
    def terminal(self):
        return JobState.is_terminal(self.state)

    @property
    def ok(self):
        return self.state == JobState.SUCCESS


@dataclass
class JobHandle:
    """An opaque, picklable reference to a submitted statement or job."""

    backend: str
    job_id: str
    ui_url: Optional[str] = None
    extra: Dict[str, Any] = field(default_factory=dict)

    def to_dict(self):
        return {
            "backend": self.backend,
            "job_id": self.job_id,
            "ui_url": self.ui_url,
            "extra": self.extra,
        }


@dataclass
class SparkJobContext:
    """Everything a backend needs in order to run one step's remote work."""

    flow: Any
    step_name: str
    pathspec: str
    flow_name: str
    run_id: str
    task_id: str
    attempt: int
    user: Optional[str]
    config: Dict[str, Any]
    tags: Dict[str, str]
    timeout_minutes: Optional[int] = None
    logger: Optional[Callable] = None

    def log(self, msg, job_id=None, stream="stdout"):
        if self.logger is not None:
            self.logger(msg, job_id=job_id, stream=stream)
        else:
            log(msg, job_id=job_id, stream=stream)


def log(msg, job_id=None, stream="stdout"):
    prefix = "spark"
    if job_id:
        prefix += "[%s]" % job_id
    print("%s: %s" % (prefix, msg), file=getattr(sys, stream), flush=True)
