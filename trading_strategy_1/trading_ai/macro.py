from __future__ import annotations

import html
import re
import xml.etree.ElementTree as ET
from datetime import datetime, timezone

import httpx

from .models import MacroRegime


class MacroNewsAnalyzer:
    HAWKISH = {
        "rate hike",
        "higher rates",
        "inflation surge",
        "inflation rises",
        "fed warns",
        "tightening",
        "recession",
        "selloff",
        "crash",
        "war",
        "sanctions",
        "default",
        "layoffs",
        "unemployment rises",
        "credit stress",
    }
    DOVISH = {
        "rate cut",
        "inflation cools",
        "soft landing",
        "stimulus",
        "rally",
        "jobs growth",
        "earnings beat",
        "recovery",
        "expansion",
        "risk-on",
        "liquidity",
    }

    def __init__(self, sources: list[str], timeout_sec: int = 10) -> None:
        self.sources = [s.strip() for s in sources if s.strip()]
        self.timeout_sec = timeout_sec

    async def analyze(self) -> MacroRegime:
        headlines = await self._fetch_headlines()
        if not headlines:
            return MacroRegime(
                score=0.0,
                mode="neutral",
                summary="No macro headlines available; keep neutral risk.",
                headline_count=0,
                fetched_at=datetime.now(timezone.utc),
            )

        raw_score = 0.0
        signals: list[str] = []
        for title in headlines:
            text = title.lower()
            for word in self.HAWKISH:
                if word in text:
                    raw_score -= 1.0
                    signals.append(f"hawkish:{word}")
            for word in self.DOVISH:
                if word in text:
                    raw_score += 1.0
                    signals.append(f"dovish:{word}")

        normalized = raw_score / max(len(headlines), 1)
        score = max(-1.0, min(1.0, normalized * 3))
        if score <= -0.2:
            mode = "risk_off"
        elif score >= 0.2:
            mode = "risk_on"
        else:
            mode = "neutral"

        signal_text = ", ".join(signals[:6]) if signals else "no dominant macro keywords"
        summary = f"Macro mode={mode} score={score:.2f}; signals={signal_text}"
        return MacroRegime(
            score=score,
            mode=mode,
            summary=summary,
            headline_count=len(headlines),
            fetched_at=datetime.now(timezone.utc),
        )

    async def _fetch_headlines(self) -> list[str]:
        headlines: list[str] = []
        async with httpx.AsyncClient(timeout=self.timeout_sec, follow_redirects=True) as client:
            for source in self.sources:
                try:
                    resp = await client.get(source)
                    resp.raise_for_status()
                except Exception:
                    continue
                headlines.extend(_extract_headlines(resp.text))
        deduped: list[str] = []
        seen: set[str] = set()
        for head in headlines:
            key = head.lower()
            if key in seen:
                continue
            seen.add(key)
            deduped.append(head)
        return deduped[:120]



def _extract_headlines(xml_text: str) -> list[str]:
    titles: list[str] = []
    text = xml_text.strip()
    if not text:
        return titles

    try:
        root = ET.fromstring(text)
        for node in root.findall(".//item/title") + root.findall(".//entry/title"):
            if node.text:
                title = html.unescape(node.text.strip())
                if title:
                    titles.append(title)
        if titles:
            return titles
    except ET.ParseError:
        pass

    for match in re.findall(r"<title>(.*?)</title>", text, flags=re.IGNORECASE | re.DOTALL):
        title = html.unescape(re.sub(r"<.*?>", "", match).strip())
        if title:
            titles.append(title)
    return titles
