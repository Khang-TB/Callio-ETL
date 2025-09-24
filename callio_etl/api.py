"""Wrapper around the Callio REST API."""
from __future__ import annotations

import time
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, Sequence, Tuple

import pandas as pd
import requests

from .config import CallioAPIConfig
from .utils import ms_to_iso, pct, safe_eval


@dataclass
class CallioAPI:
    config: CallioAPIConfig
    logger: any
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
    ) -> List[Dict[str, Any]]:
        token = self.get_token(tenant, email, password)
        if not token:
            raise RuntimeError(f"[{tenant}] Cannot obtain token")

        headers = {"token": token}
        page = 1
        all_docs: List[Dict[str, Any]] = []
        window_end_ms = int(time.time() * 1000)
        denom = max(1, window_end_ms - int(cutoff_ms or 0))

        while True:
            params = {"page": page, "pageSize": self.config.page_size, "sort": f"{time_field}DESC"}
            response = requests.get(
                f"{self.config.base_url}/{endpoint}",
                headers=headers,
                params=params,
                timeout=self.config.timeout,
            )
            if response.status_code == 401:
                self.logger.warning("%s 401 on page=%s → refreshing token", log_prefix, page)
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

            response.raise_for_status()
            payload = response.json() or {}
            docs = payload.get("docs") or []
            total_docs = payload.get("totalDocs") or payload.get("total")
            has_next = bool(payload.get("hasNextPage", False))
            count_this_page = 0
            stop_flag = False

            for doc in docs:
                timestamp = int(doc.get(time_field) or 0)
                if timestamp <= cutoff_ms:
                    stop_flag = True
                    break
                all_docs.append(doc)
                count_this_page += 1

            last_ts = int(all_docs[-1].get(time_field)) if all_docs else None
            progress = (window_end_ms - (last_ts or window_end_ms)) / denom if denom > 0 else 0.0
            self.logger.info(
                "%s page=%s got=%s cum=%s last_ts=%s window=[%s → %s] time_coverage≈%s totalDocs=%s hasNext=%s",
                log_prefix,
                page,
                count_this_page,
                len(all_docs),
                ms_to_iso(last_ts),
                ms_to_iso(cutoff_ms),
                ms_to_iso(window_end_ms),
                pct(progress),
                total_docs,
                has_next and not stop_flag,
            )

            if limit_records and len(all_docs) >= limit_records:
                all_docs = all_docs[:limit_records]
                self.logger.info("%s hit LIMIT_RECORDS_PER_ENDPOINT=%s", log_prefix, limit_records)
                break

            if stop_flag or not has_next:
                break

            page += 1

        self.logger.info(
            "%s DONE pages=%s loaded=%s range=[%s → %s]",
            log_prefix,
            page,
            len(all_docs),
            ms_to_iso(cutoff_ms),
            ms_to_iso(all_docs[0].get(time_field) if all_docs else None),
        )
        return all_docs

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
            self.logger.warning("[%s][staff] 401 → refreshing token", tenant)
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

        docs: Optional[Sequence[Dict[str, Any]]] = None
        for endpoint in ("group", "user-group"):
            response = requests.get(
                f"{self.config.base_url}/{endpoint}",
                headers={"token": token},
                timeout=self.config.timeout,
            )
            if response.status_code == 401:
                self.logger.warning("[%s][%s] 401 → refreshing token", tenant, endpoint)
                token = self.get_token(tenant, email, password, force=True)
                response = requests.get(
                    f"{self.config.base_url}/{endpoint}",
                    headers={"token": token},
                    timeout=self.config.timeout,
                )
            response.raise_for_status()
            payload = response.json() or {}
            docs = payload.get("docs") or payload
            if isinstance(docs, list):
                break
        if not isinstance(docs, list):
            docs = []
        return pd.DataFrame(docs)
