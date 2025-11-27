# Callio ETL

ETL chạy dòng lệnh: kéo dữ liệu Callio (customer, call log, staff, group) và nạp vào BigQuery. Có thể chạy một lần rồi thoát (phù hợp cron) hoặc chạy daemon theo lịch cố định hằng ngày. Mỗi lần chạy đều refresh báo cáo `fact_staff_daily_PK`.

## Yêu cầu

- Python 3.10+ và pip
- Service account BigQuery có quyền tạo dataset/table và ghi dữ liệu
- Thông tin đăng nhập tenant Callio (tenant/email/password cho từng tài khoản)
- Kết nối mạng đến Callio API và BigQuery

## Cài đặt

1) Cài dependencies

```bash
pip install -r requirements.txt
```

2) Cấu hình biến môi trường trong `.env` (hoặc env của process). Code đọc JSON inline hoặc đường dẫn file:

- `SERVICE_ACCOUNT_KEY_JSON` **hoặc** `SERVICE_ACCOUNT_KEY_FILE` (bắt buộc)
- `CALLIO_ACCOUNTS_JSON` **hoặc** `CALLIO_ACCOUNTS_FILE` (bắt buộc, danh sách `{ "tenant": "...", "email": "...", "password": "..." }`)
- Tuỳ chọn:
  - `BQ_PROJECT_ID` (mặc định `rio-system-migration`)
  - `BQ_DATASET_ID` (mặc định `dev_callio`)
  - `BQ_LOCATION` (mặc định `asia-southeast1`)
  - `LOG_LEVEL` (mặc định `INFO`)
  - `LIMIT_RECORDS_PER_ENDPOINT` (int, giới hạn số bản ghi mỗi lần gọi API)
  - `SCHEDULER_RUN_TIMES_UTC` (mặc định `02:30,04:00,06:00,08:00,11:00`)
  - `SCHEDULER_STAFF_GROUP_TIME_UTC` (mặc định: mục đầu tiên của `SCHEDULER_RUN_TIMES_UTC`)
  - `OVERLAP_MS` (mặc định `180000`, chồng lấn cửa sổ customer)
  - `DAYS_TO_FETCH_IF_EMPTY` (mặc định `30`, số ngày lùi nếu chưa có checkpoint)
  - `API_TIMEOUT`, `API_PAGE_SIZE`, `API_TIME_SLICE_MS`, `API_MIN_SLICE_MS`, `CALLIO_API_BASE_URL`

Ví dụ `.env` (dùng secret file):

```
SERVICE_ACCOUNT_KEY_FILE=service-account.json
CALLIO_ACCOUNTS_FILE=accounts.json

BQ_PROJECT_ID=rio-system-migration
BQ_DATASET_ID=dev_callio
BQ_LOCATION=asia-southeast1
LOG_LEVEL=INFO
SCHEDULER_RUN_TIMES_UTC=02:30,04:00,06:00,08:00,11:00
SCHEDULER_STAFF_GROUP_TIME_UTC=02:30
```

## Chạy

- Chạy một lần (mặc định): chạy tất cả job, refresh báo cáo, rồi thoát.

```bash
python -m callio_etl --mode once --job all
```

Giới hạn một flow: `--job customer`, `--job call`, hoặc `--job staffgroup`.

- Daemon: khởi tạo bảng/checkpoint, sau đó bám theo lịch hằng ngày. Customer + call chạy tại mỗi khung giờ `SCHEDULER_RUN_TIMES_UTC`; staff/group snapshot chạy 1 lần/ngày tại `SCHEDULER_STAFF_GROUP_TIME_UTC`.

```bash
python -m callio_etl --mode daemon
```

- Windows helper: `run_etl.cmd` chạy `--mode once --job all` và ghi log vào `callio_etl.log`.

## Ví dụ lịch

- Cron (giờ hệ thống UTC, mapping mặc định 09:30/11:00/13:00/15:00/18:00 UTC+7):

```
30 2 * * * cd /path/to/Callio-ETL && python -m callio_etl --mode once --job all >> /var/log/callio-etl.log 2>&1
0 4,6,8,11 * * * cd /path/to/Callio-ETL && python -m callio_etl --mode once --job all >> /var/log/callio-etl.log 2>&1
```

## Pipeline làm gì

- Tạo dataset/table BigQuery nếu chưa có (call_log, customer + staging/merge, staff, group, update_log)
- Lấy dữ liệu Callio API với paging theo khoảng thời gian + overlap, dedupe theo ID, tính hash, merge vào bảng partition
- Duy trì checkpoint theo tenant/table để chạy incremental
- Refresh báo cáo `fact_staff_daily_PK` sau mỗi lần chạy (once hoặc daemon) và flush update log buffer

## Ghi chú kỹ thuật (cho kỹ sư)

Bố cục mã nguồn:

- `callio_etl/config.py`: parse env cho API, BigQuery, scheduler, window, logging; hỗ trợ JSON inline hoặc đường dẫn file.
- `callio_etl/api.py`: client Callio, cache token, fetch phân trang (`fetch_desc_until`) cho customer/call, endpoint list cho staff/group.
- `callio_etl/bigquery_service.py`: bootstrap BQ; tạo schema; helper `load_append`, `load_truncate`, `execute_query`; logic staging + merge dựa trên `row_hash`.
- `callio_etl/checkpoints.py`: lưu checkpoint per tenant/table trong BQ + cache memory; `UpdateLogBuffer` gom batch ghi update_log.
- `callio_etl/runner.py`: điều phối customer, call_log, staff/group, refresh báo cáo; helper schedule và CLI entrypoint.
- `callio_etl/utils.py`: tính hash, `safe_eval` cho payload JSON/string lẫn lộn, hàm lấy user/group, sinh customField_0.
Secret qua file:

- `service-account.json`: service account Google.
- `accounts.json`: danh sách tài khoản Callio:

```json
[
  { "tenant": "TENANT1", "email": "user@example.com", "password": "secret" }
]
```

Trỏ `.env` tới các file này qua `SERVICE_ACCOUNT_KEY_FILE` và `CALLIO_ACCOUNTS_FILE`.

Callio API:

- Base URL: `CALLIO_API_BASE_URL` (mặc định `https://clientapi.phonenet.io`).
- Auth: email/password mỗi tenant để lấy token (`api.py`).
- Customer: lấy giảm dần theo `updateTime`, phân trang, có `LIMIT_RECORDS_PER_ENDPOINT`, cắt cửa sổ theo `API_TIME_SLICE_MS` + `API_MIN_SLICE_MS`; overlap qua `OVERLAP_MS` để merge an toàn.
- Call log: lấy giảm dần `createTime`, dedupe `_id`, parse `fromUser` / `fromGroup` bằng `safe_eval`.
- Staff/group: snapshot full mỗi lần; staff stage rồi merge, group truncate + replace.

BigQuery:

- Dataset mặc định `rio-system-migration.dev_callio` (có thể override).
- Bảng: `customer` (partition DATE trên `NgayUpdate`), staging `stg_customer` để merge theo cửa sổ; `call_log` partition trên `NgayTao`; `staff`, `group`, `update_log`.
- Checkpoint lưu trong `update_log`, key theo tenant/table, cập nhật sau mỗi lần merge/load.
- Reporting: `runner.run_fact_staff_daily_pk_refresh` thực thi MERGE vào `fact_staff_daily_PK` mỗi lần job chạy.

Cấu trúc bảng BigQuery (tự tạo nếu chưa có):

- `call_log`: `_id, chargeTime, createTime, direction, fromNumber, toNumber, startTime, endTime, duration, billDuration, hangupCause, answerTime, fromUser__id, fromUser__name, fromGroup__id, tenant, row_hash, NgayTao` (partition DATE trên `NgayTao`, cluster theo `tenant`).
- `customer`: `_id, assignedTime, createTime, updateTime, name, phone, user_id, user_name, user_group_id, tenant, row_hash, customField_0_val, NgayUpdate (partition DATE), NgayAssign` (cluster `tenant,_id`).
- `stg_customer`: cùng schema `customer`, dùng merge cửa sổ rồi xoá.
- `staff`: `_id, email, name, updateTime, createTime, group_id, tenant, row_hash` (cluster `tenant,name`).
- `group`: `group_id, name, tenant, row_hash` (cluster `tenant,group_id`).
- `update_log`: `table_name, tenant, updated_at (partition), rows_loaded, max_updateTime, mode`.

Luồng dữ liệu:

- Customer: lấy update mới theo tenant có overlap, load vào `stg_customer`, MERGE sang `customer` bằng bản ghi mới nhất `updateTime` per `_id`/tenant; checkpoint đặt = max `updateTime`.
- Call log: lấy giảm dần `createTime`, dedupe `_id`, tính `NgayTao` (DATE), append `call_log`, checkpoint đặt = max `createTime`.
- Staff/group: snapshot full; staff stage+merge theo `tenant,name`; group truncate+reload.
- Update log: mỗi lần load/merge append một dòng (`mode` = NOOP/STAGED/MERGED/APPEND/TRUNCATE/ERROR_LOGIN) để warm checkpoint về sau.

Chạy local vs server:

- Local: để `SERVICE_ACCOUNT_KEY_FILE`, `CALLIO_ACCOUNTS_FILE` ngoài VCS; bỏ vào `.gitignore`.
- Server: dùng env var hoặc secret mount; cron ở trên là chạy theo slot thay vì giữ process dài hạn.
