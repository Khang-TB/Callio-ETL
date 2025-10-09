"""Wrapper around the Callio REST API."""
from __future__ import annotations

import time
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, Sequence, Tuple, cast

import pandas as pd
import requests

from .config import CallioAPIConfig
from .logging_utils import progress_task
from .utils import ms_to_iso, pct, safe_eval


@dataclass
class FetchResult:
    docs: List[Dict[str, Any]]
    hit_result_window_limit: bool = False

@dataclass
class CallioAPI:
    config: CallioAPIConfig
    logger: Any
    _tokens: Dict[str, Tuple[str, float]] = field(default_factory=dict)

    # ------------------------------------------------------------------
    # Token & credential helpers
    # ------------------------------------------------------------------
    def _resolve_credentials(
        self,
        tenant: str,
        email: Optional[str],
        password: Optional[str],
    ) -> Tuple[Optional[str], Optional[str]]:
        if email and password:
            return email, password
        account = self.config.find_account(tenant)
        if account:
            return account.email, account.password
        return email, password

    def get_token(
        self,
        tenant: str,
        email: Optional[str] = None,
        password: Optional[str] = None,
        *,
        force: bool = False,
    ) -> Optional[str]:
        now = time.time()
        cached = self._tokens.get(tenant, (None, 0))
        if not force and cached[0] and now < cached[1]:
            return cached[0]

        email, password = self._resolve_credentials(tenant, email, password)
        if not email or not password:
            self.logger.error("[%s] Missing credentials", tenant)
            return None

        try:
            token = self._login(email, password)
        except Exception as exc:  # pragma: no cover - network failure path
            self.logger.error("[%s] login error: %s", tenant, exc)
            return None

        self._tokens[tenant] = (token, now + 25 * 60)
        self.logger.info("[%s] token %s", tenant, "refreshed" if force else "obtained")
        return token

    def _login(self, email: str, password: str) -> str:
        response = requests.post(
            f"{self.config.base_url}/auth/login",
            json={"email": email, "password": password},
            timeout=self.config.timeout,
        )
        response.raise_for_status()
        token = (response.json() or {}).get("token")
        if not token:
            raise RuntimeError("Cannot obtain Callio token")
        return token

    # ------------------------------------------------------------------
    # API helpers
    # ------------------------------------------------------------------
    def fetch_desc_until(
        self,
        endpoint: str,
        tenant: str,
        email: Optional[str],
        password: Optional[str],
        time_field: str,
        cutoff_ms: int,
        *,
        limit_records: Optional[int],
        log_prefix: str = "",
    ) -> FetchResult:
        token = self.get_token(tenant, email, password)
        if not token:
            raise RuntimeError(f"[{tenant}] Cannot obtain token")

        headers = {"token": token}
        window_end_ms = int(time.time() * 1000)
        denom = max(1, window_end_ms - int(cutoff_ms or 0))
        progress_label = log_prefix or f"[{tenant}][{endpoint}]"
        min_slice_ms = max(1, getattr(self.config, "min_slice_ms", 60 * 60 * 1000))
        default_slice_ms = max(0, getattr(self.config, "time_slice_ms", 24 * 60 * 60 * 1000))

        def _to_int_timestamp(value: Any) -> int:
            """Best-effort conversion of a timestamp-like value to int milliseconds."""

            if value is None:
                return 0
            if isinstance(value, (int, float)):
                return int(value)
            if isinstance(value, str):
                value = value.strip()
                if not value:
                    return 0
                try:
                    return int(float(value))
                except ValueError:
                    return 0
            try:
                return int(value)
            except (TypeError, ValueError):
                return 0

        def perform_request(params: Dict[str, Any]) -> requests.Response:
            nonlocal token, headers
            response = requests.get(
                f"{self.config.base_url}/{endpoint}",
                headers=headers,
                params=params,
                timeout=self.config.timeout,
            )
            if response.status_code == 401:
                self.logger.warning("%s 401 on page=%s -> refreshing token", log_prefix, params.get("page"))
                token = self.get_token(tenant, email, password, force=True)
                if not token:
                    raise RuntimeError(f"[{tenant}] token refresh failed")
                headers = {"token": token}
                response = requests.get(
                    f"{self.config.base_url}/{endpoint}",
                    headers=headers,
                    params=params,
                    timeout=self.config.timeout,
                )
            return response

        def fetch_slice(range_start_ms: int, range_end_ms: int) -> Tuple[List[Dict[str, Any]], bool]:
            slice_docs: List[Dict[str, Any]] = []
            slice_limit_hit = False
            page = 1
            while True:
                params: Dict[str, Any] = {
                    "page": page,
                    "pageSize": self.config.page_size,
                    "sort": f"{time_field}DESC",
                }
                if range_start_ms is not None:
                    params["from"] = max(0, int(range_start_ms))
                if range_end_ms is not None:
                    params["to"] = max(0, int(range_end_ms))

                response = perform_request(params)
                try:
                    response.raise_for_status()
                except requests.HTTPError as exc:
                    if response.status_code == 400 and "Result window is too large" in (response.text or ""):
                        slice_limit_hit = True
                        self.logger.warning(
                            "%s result window limit within slice [%s -> %s] at page=%s; collected %s rows",
                            log_prefix,
                            ms_to_iso(range_start_ms),
                            ms_to_iso(range_end_ms),
                            page,
                            len(slice_docs),
                        )
                        break
                    raise

                payload = response.json() or {}
                docs = payload.get("docs") or []
                total_docs = payload.get("totalDocs") or payload.get("total")
                has_next = bool(payload.get("hasNextPage", False))
                count_this_page = 0
                stop_flag = False

                for doc in docs:
                    timestamp = _to_int_timestamp(doc.get(time_field))
                    if timestamp <= cutoff_ms:
                        stop_flag = True
                        break
                    slice_docs.append(doc)
                    count_this_page += 1

                last_ts = _to_int_timestamp(slice_docs[-1].get(time_field)) if slice_docs else None
                self.logger.info(
                    "%s slice=[%s -> %s] page=%s got=%s cum=%s last_ts=%s totalDocs=%s hasNext=%s",
                    log_prefix,
                    ms_to_iso(range_start_ms),
                    ms_to_iso(range_end_ms),
                    page,
                    count_this_page,
                    len(slice_docs),
                    ms_to_iso(last_ts),
                    total_docs,
                    has_next and not stop_flag,
                )

                if stop_flag or not has_next:
                    break

                page += 1

            return slice_docs, slice_limit_hit

        def initial_slices(start_ms: int, end_ms: int) -> List[Tuple[int, int]]:
            if default_slice_ms == 0:
                return [(start_ms, end_ms)]
            slices: List[Tuple[int, int]] = []
            cursor_end = end_ms
            while cursor_end > start_ms:
                cursor_start = max(start_ms, cursor_end - default_slice_ms)
                slices.append((cursor_start, cursor_end))
                cursor_end = cursor_start - 1
            return slices or [(start_ms, end_ms)]

        slice_stack = list(reversed(initial_slices(cutoff_ms, window_end_ms)))
        doc_store: Dict[str, Dict[str, Any]] = {}
        limit_hit = False
        processed = 0

        with progress_task(f"{progress_label} fetching", transient=False) as (bar, task_id):
            bar.update(task_id, description=f"{progress_label} initializing")

            def add_docs(docs: Sequence[Dict[str, Any]]) -> bool:
                nonlocal processed
                for doc in docs:
                    timestamp = _to_int_timestamp(doc.get(time_field))
                    if timestamp <= cutoff_ms:
                        continue
                    key = doc.get("_id") or f"{timestamp}:{doc.get('id') or len(doc_store)}"
                    if key in doc_store:
                        continue
                    doc_store[key] = doc
                    processed += 1
                    coverage = (window_end_ms - timestamp) / denom if denom > 0 else 0.0
                    bar.update(
                        task_id,
                        advance=1,
                        description=f"{progress_label} loaded={processed} coverage={pct(coverage)}",
                    )
                    if limit_records and processed >= limit_records:
                        return True
                return False

            while slice_stack:
                range_start_ms, range_end_ms = slice_stack.pop()
                if range_end_ms <= range_start_ms:
                    continue

                bar.update(
                    task_id,
                    description=(
                        f"{progress_label} slice[{ms_to_iso(range_start_ms)} -> {ms_to_iso(range_end_ms)}] "
                        f"loaded={processed}"
                    ),
                )

                slice_docs, slice_limit_hit = fetch_slice(range_start_ms, range_end_ms)

                if add_docs(slice_docs):
                    slice_stack.clear()
                    break

                if slice_limit_hit:
                    limit_hit = True
                    oldest_ts = min(
                        (
                            _to_int_timestamp(doc.get(time_field))
                            for doc in slice_docs
                            if doc.get(time_field) is not None
                        ),
                        default=None,
                    )
                    if oldest_ts is not None and oldest_ts > range_start_ms:
                        next_end = max(range_start_ms, oldest_ts - 1)
                        if next_end > range_start_ms:
                            slice_stack.append((range_start_ms, next_end))
                            continue

                    span = range_end_ms - range_start_ms
                    if span > min_slice_ms:
                        mid = range_start_ms + span // 2
                        if mid > range_start_ms:
                            slice_stack.append((range_start_ms, mid))
                        if mid + 1 < range_end_ms:
                            slice_stack.append((mid + 1, range_end_ms))
                    else:
                        self.logger.warning(
                            "%s cannot split slice [%s -> %s] further; some rows may remain un-fetched.",
                            log_prefix,
                            ms_to_iso(range_start_ms),
                            ms_to_iso(range_end_ms),
                        )

            bar.update(task_id, description=f"{progress_label} done")

        all_docs = sorted(doc_store.values(), key=lambda doc: _to_int_timestamp(doc.get(time_field)), reverse=True)
        if limit_records:
            all_docs = all_docs[:limit_records]

        status_note = " (API result window limit hit)" if limit_hit else ""
        self.logger.info(
            "%s DONE loaded=%s range=[%s -> %s]%s",
            log_prefix,
            len(all_docs),
            ms_to_iso(cutoff_ms),
            ms_to_iso(all_docs[0].get(time_field) if all_docs else None),
            status_note,
        )
        return FetchResult(list(all_docs), limit_hit)

    def fetch_staff(self, tenant: str) -> pd.DataFrame:
        account = self.config.find_account(tenant)
        email = account.email if account else None
        password = account.password if account else None

        token = self.get_token(tenant, email, password)
        if not token:
            return pd.DataFrame()

        response = requests.get(
            f"{self.config.base_url}/user",
            headers={"token": token},
            timeout=self.config.timeout,
        )
        if response.status_code == 401:
            self.logger.warning("[%s][staff] 401 -> refreshing token", tenant)
            token = self.get_token(tenant, email, password, force=True)
            response = requests.get(
                f"{self.config.base_url}/user",
                headers={"token": token},
                timeout=self.config.timeout,
            )
        response.raise_for_status()
        docs = (response.json() or {}).get("docs") or response.json() or []
        if not isinstance(docs, list):
            docs = []
        return pd.DataFrame(docs)

    def fetch_group(self, tenant: str) -> pd.DataFrame:
        account = self.config.find_account(tenant)
        email = account.email if account else None
        password = account.password if account else None
        token = self.get_token(tenant, email, password)
        if not token:
            return pd.DataFrame()

        endpoint_url = "https://clientapi.phonenet.io/user-group"
        response = requests.get(
            endpoint_url,
            headers={"token": token},
            timeout=self.config.timeout,
        )
        if response.status_code == 401:
            self.logger.warning("[%s][%s] 401 -> refreshing token", tenant, endpoint_url)
            token = self.get_token(tenant, email, password, force=True)
            response = requests.get(
                endpoint_url,
                headers={"token": token},
                timeout=self.config.timeout,
            )
        response.raise_for_status()
        payload = response.json() or {}
        docs_seq: Sequence[Dict[str, Any]] = []
        if isinstance(payload, dict):
            docs_candidate = payload.get("docs")
            if isinstance(docs_candidate, list):
                docs_seq = docs_candidate
        elif isinstance(payload, list):
            docs_seq = cast(Sequence[Dict[str, Any]], payload)
        return pd.DataFrame(list(docs_seq))
