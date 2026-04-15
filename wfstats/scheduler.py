from __future__ import annotations

import logging
import os
import time
from dataclasses import dataclass
from threading import Lock, Thread

from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.interval import IntervalTrigger

from wfstats.helpers import get_con
from wfstats.ingestion.calculate import calculate_relic_ev
from wfstats.ingestion.market import sync_market_orders

log = logging.getLogger(__name__)

MARKET_REFRESH_SECONDS = int(os.getenv("WFSTATS_MARKET_REFRESH_SECONDS", "60"))
MARKET_REFRESH_MAX_AGE_SECONDS = int(os.getenv("WFSTATS_MARKET_REFRESH_MAX_AGE_SECONDS", "3600"))
MARKET_STARTUP_FORCE_REFRESH_AGE_SECONDS = int(os.getenv("WFSTATS_MARKET_STARTUP_FORCE_REFRESH_AGE_SECONDS", "86400"))
MARKET_REFRESH_BATCH_SIZE = int(os.getenv("WFSTATS_MARKET_BATCH_SIZE", "0"))


@dataclass
class SchedulerStatus:
    running: bool
    refresh_in_progress: bool
    last_market_refresh_at: int | None
    market_refresh_seconds: int
    market_refresh_max_age_seconds: int
    market_startup_force_refresh_age_seconds: int
    market_batch_size: int


_scheduler: BackgroundScheduler | None = None
_job_lock = Lock()


def _last_market_refresh_at() -> int | None:
    conn = get_con()
    try:
        row = conn.execute(
            """
            SELECT MAX(fetched_at) AS fetched_at
            FROM sync_log
            WHERE source = 'wfm_orders'
            """
        ).fetchone()
        return int(row["fetched_at"]) if row and row["fetched_at"] is not None else None
    finally:
        conn.close()


def refresh_market_prices(force: bool = False) -> dict[str, object]:
    if not _job_lock.acquire(blocking=False):
        log.info("Skipping scheduled market refresh; previous run still active")
        return {"skipped": True, "reason": "job already running"}

    try:
        last_refresh_at = _last_market_refresh_at()
        now = int(time.time())
        if not force and last_refresh_at is not None and now - last_refresh_at < MARKET_REFRESH_MAX_AGE_SECONDS:
            age_seconds = now - last_refresh_at
            log.info("Skipping scheduled market refresh; cache age %ss is below %ss", age_seconds, MARKET_REFRESH_MAX_AGE_SECONDS)
            return {
                "skipped": True,
                "reason": "cache still fresh",
                "cache_age_seconds": age_seconds,
                "max_age_seconds": MARKET_REFRESH_MAX_AGE_SECONDS,
            }

        market_result = sync_market_orders(limit=MARKET_REFRESH_BATCH_SIZE)
        ev_result = calculate_relic_ev()
        result = {
            "market": market_result,
            "relic_ev": ev_result,
        }
        log.info("Scheduled market refresh complete: %s", result)
        return result
    except Exception:
        log.exception("Scheduled market refresh failed")
        raise
    finally:
        _job_lock.release()


def should_force_startup_refresh() -> bool:
    last_refresh_at = _last_market_refresh_at()
    if last_refresh_at is None:
        return True
    return int(time.time()) - last_refresh_at >= MARKET_STARTUP_FORCE_REFRESH_AGE_SECONDS


def start_background_market_refresh(force: bool = False) -> bool:
    if _job_lock.locked():
        log.info("Skipping background market refresh start; refresh already running")
        return False

    def _runner() -> None:
        try:
            refresh_market_prices(force=force)
        except Exception:
            log.exception("Background market refresh failed")

    Thread(target=_runner, name="wfstats-market-refresh", daemon=True).start()
    return True


def start_scheduler() -> SchedulerStatus:
    global _scheduler

    if _scheduler is not None and _scheduler.running:
        return get_scheduler_status()

    _scheduler = BackgroundScheduler()
    _scheduler.add_job(
        refresh_market_prices,
        trigger=IntervalTrigger(seconds=MARKET_REFRESH_SECONDS),
        id="market-refresh",
        max_instances=1,
        coalesce=True,
        replace_existing=True,
    )
    _scheduler.start()
    log.info(
        "Scheduler started: poll every %ss, refresh when cache older than %ss, batch size %s",
        MARKET_REFRESH_SECONDS,
        MARKET_REFRESH_MAX_AGE_SECONDS,
        MARKET_REFRESH_BATCH_SIZE,
    )
    return get_scheduler_status()


def stop_scheduler() -> None:
    global _scheduler

    if _scheduler is None:
        return

    _scheduler.shutdown(wait=False)
    _scheduler = None
    log.info("Scheduler stopped")


def get_scheduler_status() -> SchedulerStatus:
    return SchedulerStatus(
        running=bool(_scheduler and _scheduler.running),
        refresh_in_progress=_job_lock.locked(),
        last_market_refresh_at=_last_market_refresh_at(),
        market_refresh_seconds=MARKET_REFRESH_SECONDS,
        market_refresh_max_age_seconds=MARKET_REFRESH_MAX_AGE_SECONDS,
        market_startup_force_refresh_age_seconds=MARKET_STARTUP_FORCE_REFRESH_AGE_SECONDS,
        market_batch_size=MARKET_REFRESH_BATCH_SIZE,
    )
