"""Shared utility functions for the Callio ETL pipeline."""
from __future__ import annotations

import ast
import hashlib
import json
import re
import unicodedata
from difflib import SequenceMatcher
from datetime import datetime, timedelta, timezone
from typing import Any, Iterable, List, Optional

import pandas as pd


def safe_eval(value: Any) -> Any:
    """Safely attempt to parse JSON/AST literals, falling back to the original value."""
    if isinstance(value, str):
        try:
            return json.loads(value)
        except Exception:
            try:
                return ast.literal_eval(value)
            except Exception:
                return value
    return value


def ensure_unique_columns(df: pd.DataFrame) -> pd.DataFrame:
    """Ensure a DataFrame has unique column names by suffixing duplicates."""
    df = df.copy()
    df.columns = [str(col) for col in df.columns]
    counts = {}
    new_columns: List[str] = []
    for col in df.columns:
        count = counts.get(col, 0)
        if count:
            new_columns.append(f"{col}__{count}")
        else:
            new_columns.append(col)
        counts[col] = count + 1
    df.columns = new_columns
    return df


def compute_row_hash(df: pd.DataFrame) -> pd.Series:
    if df.empty:
        return pd.Series([], dtype="string")
    volatile = {
        "row_hash",
        "updateTime",
        "createTime",
        "updatedAt",
        "createdAt",
        "NgayTao",
        "NgayUpdate",
        "NgayAssign",
    }
    cols = [col for col in df.columns if col not in volatile]

    def _hash(row: pd.Series) -> str:
        payload = {col: row.get(col) for col in cols}
        as_bytes = json.dumps(payload, sort_keys=True, default=str).encode("utf-8")
        return hashlib.md5(as_bytes).hexdigest()

    return df[cols].apply(lambda r: _hash(r), axis=1)


def yyyymm_from_ms(ms: Optional[int]) -> str:
    if ms is None:
        return datetime.now(timezone.utc).strftime("%Y%m")
    return datetime.fromtimestamp(ms / 1000, tz=timezone.utc).strftime("%Y%m")


def ms_to_iso(ms: Optional[int]) -> str:
    if ms is None:
        return "None"
    try:
        return datetime.fromtimestamp(int(ms) / 1000, tz=timezone.utc).isoformat()
    except Exception:
        return str(ms)


def pct(value: float) -> str:
    try:
        bounded = max(0.0, min(1.0, value))
        return f"{bounded * 100:.1f}%"
    except Exception:
        return "N/A"


def iso_week_key(dt: datetime) -> str:
    iso = dt.isocalendar()
    year = getattr(iso, "year", iso[0])
    week = getattr(iso, "week", iso[1])
    return f"{int(year)}w{int(week):02d}"


def week_start_vn(dt: datetime) -> datetime.date:
    local = dt + timedelta(hours=7)
    return local.date() - timedelta(days=local.isoweekday() - 1)


def derive_cf0_string_from_df(df: pd.DataFrame) -> pd.Series:
    if "customFields" not in df.columns:
        return pd.Series([None] * len(df), dtype="string")

    target_key = "tinh-trang-kh"

    def pick(value: Any) -> Optional[str]:
        parsed = safe_eval(value)
        if isinstance(parsed, list):
            items = parsed
        elif isinstance(parsed, dict):
            items = [parsed]
        elif parsed is None:
            items = []
        else:
            items = [parsed]
        if not items:
            return None

        match: Optional[dict] = None
        for item in items:
            if not isinstance(item, dict):
                continue
            key = str(item.get("key") or "").strip().lower()
            if key == target_key:
                match = item
                break
        if match is None:
            return None

        raw = match.get("val") or match.get("value") or match.get("values") or match.get("text") or match.get("name")
        if raw is None:
            return None

        if isinstance(raw, list):
            values = [str(elem).strip() for elem in raw if elem is not None and str(elem).strip()]
        else:
            candidate = str(raw).strip()
            values = [candidate] if candidate else []
        if not values:
            return None

        unique: List[str] = []
        seen: set[str] = set()
        for val in values:
            if val in seen:
                continue
            seen.add(val)
            unique.append(val)
        return " | ".join(unique) if unique else None

    return df["customFields"].apply(pick).astype("string")


def extract_user_id(df: pd.DataFrame) -> pd.Series:
    if "user" not in df.columns:
        return pd.Series([None] * len(df), dtype="string")

    def _extract(value: Any) -> Optional[str]:
        parsed = safe_eval(value)
        if isinstance(parsed, dict):
            return parsed.get("_id") or parsed.get("id")
        if isinstance(parsed, (list, tuple)):
            try:
                as_dict = dict(parsed)
                return as_dict.get("_id") or as_dict.get("id")
            except Exception:
                return None
        return None

    return df["user"].apply(_extract).astype("string")


def extract_user_name(df: pd.DataFrame) -> pd.Series:
    if "user" not in df.columns:
        return pd.Series([None] * len(df), dtype="string")

    def _extract(value: Any) -> Optional[str]:
        parsed = safe_eval(value)
        if isinstance(parsed, dict):
            return parsed.get("name")
        return None

    return df["user"].apply(_extract).astype("string")


def extract_user_group_id(df: pd.DataFrame) -> pd.Series:
    if "user" not in df.columns:
        return pd.Series([None] * len(df), dtype="string")

    def _extract(value: Any) -> Optional[str]:
        parsed = safe_eval(value)
        if isinstance(parsed, dict):
            group = parsed.get("group")
            if isinstance(group, dict):
                return group.get("_id") or group.get("id")
            return group
        return None

    return df["user"].apply(_extract).astype("string")
