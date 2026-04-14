from __future__ import annotations

import logging
import os
from dataclasses import dataclass
from threading import Lock

from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.interval import IntervalTrigger

from wfstats.ingestion.calculate import calculate_relic_ev
from wfstats.ingestion.market import sync_market_orders

log = logging.getLogger(__name__)

MARKET_REFRESH_SECONDS = int(os.getenv("WFSTATS_MARKET_REFRESH_SECONDS", "70"))
MARKET_REFRESH_BATCH_SIZE = int(os.getenv("WFSTATS_MARKET_BATCH_SIZE", "1"))


@dataclass
class SchedulerStatus:
    running: bool
    market_refresh_seconds: int
    market_batch_size: int


_scheduler: BackgroundScheduler | None = None
_job_lock = Lock()


def refresh_market_prices() -> dict[str, object]:
    if not _job_lock.acquire(blocking=False):
        log.info("Skipping scheduled market refresh; previous run still active")
        return {"skipped": True, "reason": "job already running"}

    try:
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
        "Scheduler started: market refresh every %ss, batch size %s",
        MARKET_REFRESH_SECONDS,
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
        market_refresh_seconds=MARKET_REFRESH_SECONDS,
        market_batch_size=MARKET_REFRESH_BATCH_SIZE,
    )
