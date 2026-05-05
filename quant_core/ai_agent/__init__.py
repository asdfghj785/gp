"""Local AI agent helpers for 14:46 sentiment and risk interviews."""


def run_1446_ai_interview(*args, **kwargs):
    from .agent_gateway import run_1446_ai_interview as _run

    return _run(*args, **kwargs)

__all__ = ["run_1446_ai_interview"]
