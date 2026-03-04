from __future__ import annotations

import asyncio
import logging
import signal

from .config import settings
from .grandmother import GrandmotherRuntime
from .notify import notify_mac


LOG_FORMAT = "%(asctime)s %(levelname)s %(name)s %(message)s"
RESTART_DELAY_SEC = 30
SUPERVISOR_POLL_SEC = 2


async def run() -> None:
    logging.basicConfig(level=getattr(logging, settings.log_level.upper(), logging.INFO), format=LOG_FORMAT)

    # Default to quiet console output for everything except explicit intent/trade logs.
    root = logging.getLogger()
    root.setLevel(logging.WARNING)

    # Allow these two dedicated loggers to emit INFO level messages so we only see
    # child intent submissions and executed trades on the console.
    logging.getLogger("trading_ai.intent").setLevel(logging.INFO)
    logging.getLogger("trading_ai.trade").setLevel(logging.INFO)

    logger = logging.getLogger("trading_ai")

    if not settings.alpaca_api_key or not settings.alpaca_secret_key:
        raise RuntimeError("Missing ALPACA_API_KEY or ALPACA_SECRET_KEY in .env")

    stop_event = asyncio.Event()

    def _shutdown() -> None:
        if not stop_event.is_set():
            logger.info("shutdown_signal_received")
            stop_event.set()

    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, _shutdown)

    while not stop_event.is_set():
        gm: GrandmotherRuntime | None = None
        try:
            gm = GrandmotherRuntime(settings)
            await gm.start()
            logger.info("hive_running")

            while not stop_event.is_set():
                if gm.unauthorized_halt:
                    logger.error("alpaca_unauthorized_halt_active")
                    await asyncio.sleep(SUPERVISOR_POLL_SEC)
                    continue

                if gm.restart_requested:
                    reason = gm.restart_reason or "ConnectionError"
                    logger.warning("connection_restart_scheduled_30s reason=%s", reason)
                    await notify_mac(
                        title="TradingAI Auto-Restart",
                        subtitle="ConnectionError detected",
                        message=f"Restarting hive in {RESTART_DELAY_SEC}s.",
                    )
                    await gm.stop()
                    gm = None
                    if stop_event.is_set():
                        break
                    await asyncio.sleep(RESTART_DELAY_SEC)
                    break

                await asyncio.sleep(SUPERVISOR_POLL_SEC)

        except Exception as exc:  # noqa: BLE001
            logger.exception("runtime_supervisor_crash error=%s", exc)
            text = str(exc).lower()
            if "401" in text or "unauthorized" in text:
                await notify_mac(
                    title="TradingAI Safety Halt",
                    subtitle="Alpaca Unauthorized (401)",
                    message="Runtime paused. Update API credentials and restart.",
                )
                while not stop_event.is_set():
                    await asyncio.sleep(5)
            elif not stop_event.is_set():
                logger.warning("supervisor_restart_in_30s")
                await asyncio.sleep(RESTART_DELAY_SEC)

        finally:
            if gm is not None:
                try:
                    await gm.stop()
                except Exception as stop_exc:  # noqa: BLE001
                    logger.exception("runtime_stop_error error=%s", stop_exc)



def main() -> None:
    asyncio.run(run())
