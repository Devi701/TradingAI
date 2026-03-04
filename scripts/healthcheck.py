from __future__ import annotations

import asyncio
import json
import sys
from pathlib import Path

sys.path.append(str(Path(__file__).resolve().parents[1]))

from trading_ai.alpaca_gateway import AlpacaGateway
from trading_ai.config import settings
from trading_ai.macro import MacroNewsAnalyzer


async def _macro_preview() -> dict[str, object]:
    analyzer = MacroNewsAnalyzer(settings.macro_news_sources)
    regime = await analyzer.analyze()
    return {
        "mode": regime.mode,
        "score": regime.score,
        "headline_count": regime.headline_count,
        "summary": regime.summary,
    }



def main() -> None:
    required = {
        "ALPACA_API_KEY": bool(settings.alpaca_api_key and "your_alpaca" not in settings.alpaca_api_key),
        "ALPACA_SECRET_KEY": bool(settings.alpaca_secret_key and "your_alpaca" not in settings.alpaca_secret_key),
        "ALPACA_BASE_URL": bool(settings.alpaca_base_url),
    }
    if not all(required.values()):
        raise SystemExit(f"Missing required env values: {json.dumps(required)}")

    Path(settings.hive_state_db_path).parent.mkdir(parents=True, exist_ok=True)
    gateway = AlpacaGateway(settings)
    acct = gateway.get_account()

    stock_symbols = settings.all_stock_symbols[:2]
    crypto_symbols = settings.all_crypto_symbols[:2]
    stock_snaps = gateway.get_stock_snapshots(stock_symbols) if stock_symbols else {}
    crypto_snaps = gateway.get_crypto_snapshots(crypto_symbols) if crypto_symbols else {}

    macro = asyncio.run(_macro_preview()) if settings.grandmother_macro_news_enabled else {"mode": "disabled"}

    print(
        json.dumps(
            {
                "status": "ok",
                "account": acct.account_number,
                "equity": acct.equity,
                "base_url": settings.alpaca_base_url,
                "data_url": settings.alpaca_data_url,
                "cache_backend": settings.snapshot_cache_backend,
                "db": settings.hive_state_db_path,
                "sample_stock_snapshots": list(stock_snaps.keys()),
                "sample_crypto_snapshots": list(crypto_snaps.keys()),
                "macro_preview": macro,
            },
            indent=2,
        )
    )


if __name__ == "__main__":
    main()
