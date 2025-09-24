"""Helpers for reading rank mapping spreadsheets."""
from __future__ import annotations

import json
import re
from typing import Any, List, Optional

import pandas as pd
from google.oauth2 import service_account

try:  # optional dependency during tests
    import gspread
except ModuleNotFoundError:  # pragma: no cover - gspread optional
    gspread = None

from .config import RankMappingConfig


def _strip_accents_upper(text: Any) -> str:
    import unicodedata

    normalized = unicodedata.normalize("NFD", str(text or ""))
    return "".join(ch for ch in normalized if unicodedata.category(ch) != "Mn").upper().strip()


def _extract_code(token: str) -> Optional[str]:
    normalized = _strip_accents_upper(token)
    match = re.search(r"\b([A-Z]{2}\d{2})\b", normalized)
    return match.group(1) if match else None


def _clean_grade(token: str) -> Optional[str]:
    normalized = _strip_accents_upper(token)
    match = re.search(r"([A-Z])", normalized)
    return match.group(1) if match else (normalized or None)


def _df_from_block(rows: List[List[Any]]) -> pd.DataFrame:
    if not rows or len(rows) < 2:
        return pd.DataFrame(columns=["code", "grade", "target_day", "target_week", "target_month"])

    header = rows[0]
    body = rows[1:]
    width_body = max((len(row) for row in body), default=len(header))
    width = max(len(header), width_body)
    header = (header + [None] * (width - len(header)))[:width]
    body_normalized = [(row + [None] * (width - len(row)))[:width] for row in body]

    normalized_header = [_strip_accents_upper(value) for value in header]

    def find_col(words_any=None, must_have=None):
        def norm(token: Any) -> str:
            return _strip_accents_upper(token)

        for idx, value in enumerate(normalized_header):
            ok = True
            if must_have:
                ok = all(norm(item) in value for item in must_have)
            if ok and words_any:
                ok = any(norm(token) in value for token in words_any)
            if ok:
                return idx
        return None

    code_idx = find_col(words_any=("MA NV", "MA NHAN VIEN", "NHAN VIEN", "MA", "CODE"))
    grade_idx = find_col(words_any=("HANG", "CAP", "BAC", "RANK", "GRADE"))
    day_idx = find_col(words_any=("TARGET", "CHI TIEU", "MUC TIEU", "CT"), must_have=("NGAY",))
    week_idx = find_col(words_any=("TARGET", "CHI TIEU", "MUC TIEU", "CT"), must_have=("TUAN",))
    month_idx = find_col(words_any=("TARGET", "CHI TIEU", "MUC TIEU", "CT"), must_have=("THANG",))

    frame = pd.DataFrame(body_normalized, columns=range(width))
    if code_idx is None:
        hits = {i: frame[i].astype(str).map(_extract_code).notna().sum() for i in range(width)}
        code_idx = max(hits, key=hits.get) if hits and max(hits.values()) > 0 else None
    if grade_idx is None:
        hits = {i: frame[i].astype(str).map(_clean_grade).notna().sum() for i in range(width)}
        grade_idx = max(hits, key=hits.get) if hits and max(hits.values()) > 0 else None
    if code_idx is None:
        return pd.DataFrame(columns=["code", "grade", "target_day", "target_week", "target_month"])

    def to_int(value: Any) -> Optional[int]:
        string = str(value or "").strip().replace(" ", "").replace(",", "").replace(".", "")
        matches = re.findall(r"\d+", string)
        return int("".join(matches)) if matches else None

    output = pd.DataFrame(
        {
            "code": frame[code_idx].map(_extract_code),
            "grade": frame[grade_idx].map(_clean_grade) if grade_idx is not None else None,
            "target_day": frame[day_idx].map(to_int) if day_idx is not None else None,
            "target_week": frame[week_idx].map(to_int) if week_idx is not None else None,
            "target_month": frame[month_idx].map(to_int) if month_idx is not None else None,
        }
    )
    output = output[output["code"].notna()].drop_duplicates("code", keep="first")
    return output


def read_rank_mapping(config: RankMappingConfig, service_account_json: str) -> pd.DataFrame:
    if gspread is None:
        raise RuntimeError("gspread is required for rank mapping but is not installed")

    info = json.loads(service_account_json)
    scopes = ["https://www.googleapis.com/auth/spreadsheets.readonly"]
    credentials = service_account.Credentials.from_service_account_info(info, scopes=scopes)
    client = gspread.authorize(credentials)
    worksheet = client.open_by_key(config.sheet_id).worksheet(config.tab_name)

    parts: List[pd.DataFrame] = []
    for cell_range in config.ranges:
        rows = worksheet.get(cell_range)
        block = _df_from_block(rows)
        if not block.empty:
            parts.append(block)
    if not parts:
        return pd.DataFrame(columns=["code", "grade"])
    return pd.concat(parts, ignore_index=True).drop_duplicates("code", keep="first")
