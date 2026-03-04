from __future__ import annotations

import asyncio
import subprocess


async def notify_mac(title: str, message: str, subtitle: str = "") -> None:
    await asyncio.to_thread(_notify_mac_sync, title, message, subtitle)



def _notify_mac_sync(title: str, message: str, subtitle: str) -> None:
    safe_title = title.replace('"', "'")
    safe_message = message.replace('"', "'")
    safe_subtitle = subtitle.replace('"', "'")
    if safe_subtitle:
        script = f'display notification "{safe_message}" with title "{safe_title}" subtitle "{safe_subtitle}"'
    else:
        script = f'display notification "{safe_message}" with title "{safe_title}"'
    try:
        subprocess.run(["osascript", "-e", script], check=False, capture_output=True, text=True)
    except Exception:
        # Fallback: no-op if desktop notifications are unavailable.
        return
