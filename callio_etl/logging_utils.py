"""Logging helpers for the Callio ETL pipeline."""
from __future__ import annotations

import logging
from contextlib import contextmanager
from typing import Iterable, Iterator, Optional, Tuple, TypeVar

from rich.console import Console
from rich.logging import RichHandler
from rich.progress import (
    BarColumn,
    Progress,
    ProgressColumn,
    SpinnerColumn,
    TaskProgressColumn,
    TextColumn,
    TimeElapsedColumn,
    TimeRemainingColumn,
)
from rich.traceback import install

install(show_locals=False)

_console: Optional[Console] = None
T = TypeVar("T")


def _get_console() -> Console:
    """Return a shared Rich console instance."""
    global _console
    if _console is None:
        _console = Console()
    return _console


def _progress_columns() -> Tuple[ProgressColumn, ...]:
    """Common column layout for progress bars."""
    return (
        SpinnerColumn(style="bold cyan"),
        TextColumn("[progress.description]{task.description}", justify="left"),
        BarColumn(bar_width=None),
        TaskProgressColumn(),
        TimeElapsedColumn(),
        TimeRemainingColumn(),
    )


def _create_progress(*, transient: bool = False) -> Progress:
    """Instantiate a Rich progress bar bound to the shared console."""
    return Progress(*_progress_columns(), console=_get_console(), transient=transient)


def configure_logging(level: str) -> logging.Logger:
    """Configure and return the root logger used across the application."""

    handler = RichHandler(
        console=_get_console(),
        show_time=True,
        show_path=False,
        markup=True,
        rich_tracebacks=True,
    )
    logging.basicConfig(level=level, handlers=[handler], format="%(message)s", force=True)
    return logging.getLogger("callio_etl")


@contextmanager
def progress_task(
    description: str,
    *,
    total: Optional[float] = None,
    transient: bool = False,
):
    """Context manager yielding a Rich progress bar task."""

    progress = _create_progress(transient=transient)
    with progress as bar:
        task_id = bar.add_task(description, total=total)
        yield bar, task_id


def track_progress(
    iterable: Iterable[T],
    description: str,
    *,
    total: Optional[int] = None,
    transient: bool = False,
) -> Iterator[T]:
    """Yield items from *iterable* while displaying a progress bar."""

    if total is None:
        try:
            total = len(iterable)  # type: ignore[arg-type]
        except TypeError:
            total = None

    progress = _create_progress(transient=transient)

    def _iterator() -> Iterator[T]:
        with progress as bar:
            task_id = bar.add_task(description, total=total)
            for item in iterable:
                yield item
                bar.advance(task_id)

    return _iterator()


__all__ = ["configure_logging", "progress_task", "track_progress"]
