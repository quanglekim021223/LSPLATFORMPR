# Fabric Bronze — runtime và deploy

Code chạy bằng Python 3.11+ trên Function App. Credentials vendor lấy trực tiếp
từ environment/App Settings (Key Vault reference được Azure resolve). Timer không
chạy `az login` hoặc đọc settings của một Function App khác.

## Luồng đã nối

1. Timer hoặc API ingestion chọn vendor được cấu hình trong `FABRIC_VENDORS`.
2. Managed Identity giữ Blob lease riêng cho vendor và tải checkpoint đã commit.
3. Nếu còn batch publish dở, tiếp tục batch đó trước; không gọi vendor lại.
4. Nếu không còn batch dở, pull theo checkpoint vào thư mục tạm của invocation.
5. Chỉ khi toàn bộ domain của vendor thành công: kiểm tra checksum/số record,
   chuyển JSON/CSV thành Parquet dạng bảng ở runtime.
6. Lưu batch Parquet + checkpoint ứng viên trong Blob state để có thể retry.
7. Append từng bảng vào Bronze bằng Delta transaction và lưu chính xác Delta
   version/count; nếu có thay đổi ngoài luồng, lần chạy sau restore version đã commit.
8. Chỉ sau khi tất cả bảng publish thành công mới commit checkpoint bền vững.

State nằm ở Azure Blob Storage, không nằm trong Fabric `Files`. Raw chỉ tồn tại
trong thư mục tạm và được dọn khi invocation kết thúc bình thường. Mỗi vendor có
Blob `<workspace>/<lakehouse>/<schema>/<vendor>/state.zip` trong container state.

## Bản đồ code cần đọc

Đọc theo thứ tự này để hiểu đường chạy production:

1. `function_app.py`: timer Azure bắt đầu ingestion và giới hạn số vendor chạy
   đồng thời.
2. `src/app/main.py`: chọn 8 vendor runner; khi `FABRIC_ENABLED=true` thì bọc mỗi
   runner bằng Fabric job.
3. `src/app/fabric_job.py`: điều phối transaction: lấy state → pull incremental →
   build batch → publish Delta → commit checkpoint.
4. `src/app/fabric_state.py`: giữ checkpoint/pending batch bền vững trong Azure
   Blob và dùng lease để ngăn hai invocation cùng chạy một vendor.
5. `src/app/fabric_tables.py`: đọc raw của đúng run, chuyển thành Parquet dạng
   bảng và append idempotent vào OneLake.
6. `src/app/fabric_contract.py`: danh sách endpoint → tên 21 bảng và đường dẫn
   records trong JSON/CSV.
7. `src/app/services/<vendor>/`: logic watermark, overlap, pagination và API/SFTP
   riêng của từng vendor.
8. `src/app/fabric_seed.py`: công cụ một lần để đưa checkpoint bootstrap đã đối
   soát vào Blob trước khi bật schedule.

`fabric_runtime.py` chỉ phục vụ việc đọc và xác thực settings khi seed state;
không nằm trên đường timer production. Các file `tests/unit/test_fabric_*`
kiểm chứng retry, incremental, pagination, state và Delta append.

## Cách ghi Bronze

- Mapping đủ 20 dataset → 21 bảng, gồm Coursera Course Detail và cả SkillUp Learning Resources và
  Certificates.
- Hai bảng SkillUp này giữ schema bootstrap hiện có (`learning_material_id`,
  `certificate_id`, các cột mảng skill) để Delta append không sinh cột song song.
- Giữ record trùng nghiệp vụ, field nguồn dạng STRING; nested array/object thành
  JSON string trong cell. Mỗi row có bốn metadata `_ingested_at`, `_run_id`,
  `_source_vendor`, `_source_domain`. Checkpoint nằm trong Blob state, không phải
  bảng Bronze.
- Append các phiên bản thay đổi: 50 record ban đầu + 5 record thay đổi = 55 dòng
  Bronze. Silver do pipeline/team sở hữu Silver chọn bản mới nhất theo business
  key. Snapshot DataCamp/FAMS thay đổi cũng append snapshot mới; không có thay đổi
  thì service bỏ qua ghi batch.
- Mỗi bảng dùng một transaction app ID ổn định và generation lấy từ Blob state.
  Retry cùng generation không append lại. Không xóa/reset transaction history,
  checkpoint hoặc bật `delta.setTransactionRetentionDuration` ngắn hơn thời gian
  có thể retry; chỉ runtime này quản lý các transaction app ID đó.
- Batch rỗng không tạo bảng mới vì không có source schema; bảng hiện có được giữ.
- Bảng đã publish trước thay đổi metadata không được tự backfill: các row cũ sẽ
  có bốn cột kỹ thuật là `NULL` sau schema merge. Trước rollout chính thức, dùng
  target trống hoặc thực hiện một lần rebuild/bootstrap đã được duyệt.
- Commit atomic theo từng bảng, không atomic cho toàn bộ 21 bảng. Nếu bảng thứ hai
  lỗi, bảng thứ nhất có thể đã thấy trên Fabric; invocation sau tiếp tục batch dở.
- Fabric runtime tắt periodic full/weekly resync của Coursera, LinkedIn, SkillUp,
  DataCamp; vẫn giữ daily overlap. Sửa record nằm ngoài khoảng API lọc có thể không
  được phát hiện; muốn backfill lịch sử cũ cần chạy tác vụ riêng có chủ đích.

## App Settings cần chuẩn bị

Các giá trị workspace/lakehouse đang ghi trong tài liệu bootstrap là DEV. Không
tự dùng chúng làm production target.

DEV đã xác minh ngày 2026-09-11:

```text
FABRIC_WORKSPACE_ID=851dacd8-2ad0-42a1-8817-55d2a7682bc6
FABRIC_LAKEHOUSE_ID=3535e0e3-a2a0-4387-82e6-1e7d8cbd1a62
FABRIC_SCHEMA=dbo
FABRIC_STATE_ACCOUNT_URL=https://fsadataingestfunctionapp.blob.core.windows.net
FABRIC_STATE_CONTAINER=fabric-ingestion-state
CHECKPOINT_DB_PATH=/tmp/fsa_ingestion.db
```

Ảnh cấu hình ngày 2026-09-12 cho thấy container `fabric-ingestion-state` đã được
tạo, Managed Identity của Function App có role Storage Blob Data Contributor và
đã được thêm vào Fabric workspace với quyền Contributor. Đây mới là kiểm tra cấu
hình; vẫn cần smoke test từ chính Function App để xác nhận token, network và ghi
Delta hoạt động end-to-end.

- `FABRIC_ENABLED=true` để nối publisher vào timer và API ingestion.
- `FABRIC_WORKSPACE_ID`, `FABRIC_LAKEHOUSE_ID`: UUID của production target.
- `FABRIC_SCHEMA=dbo`: schema đã tạo sẵn trong Lakehouse.
- `FABRIC_STATE_ACCOUNT_URL=https://<account>.blob.core.windows.net`.
- `FABRIC_STATE_CONTAINER=fabric-ingestion-state`: tạo container trước.
- `FABRIC_MANAGED_IDENTITY_CLIENT_ID`: để trống cho system-assigned identity;
  điền client ID nếu dùng user-assigned identity.
- `FABRIC_VENDORS`: JSON array vendor được duyệt, ví dụ `["levelup"]` khi smoke
  test; mặc định là cả 8 vendor, thiếu cấu hình sẽ báo lỗi thay vì bỏ qua.
- `FABRIC_MAX_CONCURRENT_VENDORS=1`: giới hạn concurrency của timer.
- `FABRIC_ALLOW_INITIAL_PULL=false`: từ chối tự full-pull khi checkpoint mất.
- `CHECKPOINT_DB_PATH=/tmp/fsa_ingestion.db`: SQLite cục bộ của từng Function
  instance; checkpoint Fabric bền vững vẫn nằm trong Blob state.
- `AzureWebJobs.scheduled_vendor_ingestion.Disabled=true` trong lúc rollout.
- `INGESTION_TIMER_SCHEDULE`: Azure NCRONTAB 6 trường, ví dụ `0 0 22 * * *`
  cho 05:00 Việt Nam hằng ngày nếu host dùng UTC.
- `INGESTION_QUEUE_NAME=vendor-ingestion`: queue cho endpoint Run Now.
- `INGESTION_STATUS_CONTAINER=ingestion-job-status`: Blob container lưu trạng thái
  job để `GET /ingestions/{job_id}` đọc được từ mọi Function instance. Queue và
  container dùng connection `AzureWebJobsStorage` và được tạo khi cần.

Code production đọc target từ các biến trên. Không còn Workspace/Lakehouse ID
hardcode trong source.

Giữ credentials vendor hiện có. Cấu hình thêm các URL/base path thật đang dùng
trong bootstrap; runtime không tự gán URL theo tài khoản dev:

- LevelUP: `LEVELUP_BASE_URL`, `LEVELUP_USER_NAME`, `LEVELUP_PASSWORD`, `LEVELUP_KEY`.
- SkillUp: `SKILLUP_KEY`; runtime giới hạn page size tối đa 50.
  Assessment chia cửa sổ tối đa một ngày, tiếp tục chia nhỏ khi phân trang không
  đủ. Cửa sổ vẫn lỗi ở mức 15 phút sẽ fail, không đánh dấu pull hoàn tất.
- DataCamp: `DATACAMP_BASE_URL`, `DATACAMP_TOKEN`, `DATACAMP_EVENTS_START_TIME`.
- Coursera: `COURSERA_BASE_URL`, `COURSERA_TOKEN_URL`, `COURSERA_USER_NAME`,
  `COURSERA_PASSWORD`, `COURSERA_ORGID` hoặc `COURSERA_ORG_ID`, và
  `COURSERA_CONTENT_DETAIL_PATH_TEMPLATE=/{org_id}/contents/{id}`.
- LinkedIn: `LINKEDIN_BASE_URL`, `LINKEDIN_TOKEN_URL`, `LINKEDIN_CLIENT_ID`,
  `LINKEDIN_CLIENT_SECRET`, `LINKEDIN_HISTORY_START_TIME`,
  detail query không bắt buộc khi bật Fabric.
- Harvard: credentials API/SFTP, `HARVARD_ORGID` (hoặc org key riêng HMM/Spark),
  `HARVARD_SFTP_REMOTE_DIR`. Có thể cấu hình thêm `HARVARD_SFTP_KNOWN_HOSTS`
  trỏ tới file host keys được tin cậy và có sẵn trong runtime. Cấu hình `HARVARD_HMM_HISTORY_START_DATE`
  và `HARVARD_SPARK_HISTORY_START_DATE` nếu muốn giới hạn khoảng kiểm tra.
  Fabric quét file thực sự có trên SFTP đến ngày báo cáo gần nhất, so metadata
  để nhận cả file cũ bị sửa; để trống sẽ quét tất cả file còn lưu.
- FAMS: token, network route/allowlist từ Function App; cấu hình các filter được
  API thật chấp nhận. Service chấp nhận ngày `YYYYMMDD` và `YYYY-MM-DD`, giữ
  nguyên định dạng cấu hình khi gửi API. Bootstrap live dùng `YYYY-MM-DD`. Tránh filter ngày kết
  thúc thay đổi mỗi ngày nếu muốn fingerprint snapshot có cùng scope.

Managed Identity cần quyền ghi Lakehouse/OneLake phù hợp và Storage Blob Data
Contributor trên container state. Cấu hình authentication API hiện có vẫn cần
cho endpoint HTTP. Không gửi credentials vào MR hoặc log CI.

## Chuyển từ bootstrap sang checkpoint bền vững

Không lấy `mtime` file hay ngày hiện tại làm watermark vì có thể bỏ sót dữ liệu.
Không chọn đại checkpoint của một mock test. Checkpoint phải tương ứng dữ liệu
đã publish trong đúng production target và đủ state của tất cả domain của vendor.

Từ `backend/`, sau khi đặt Fabric target trong environment và đăng nhập Azure:

```bash
../.venv/bin/python -m app.fabric_seed \
  --vendor levelup \
  --checkpoint /absolute/path/to/audited/checkpoint.db \
  --confirm-already-published
```

Lệnh chỉ seed Blob state trống, kiểm tra latest run thành công, watermark/domain,
full-sync scope cho history và inventory của LevelUP. Nó không ghi bảng Fabric
và không overwrite checkpoint đã tồn tại.

Một số `fabric_runs/<batch>/checkpoint.db` bootstrap chỉ chứa một domain hoặc
chưa có các watermark `full_sync`: lệnh sẽ từ chối. Cần hợp nhất/migrate dựa trên
manifest đã đối soát trước khi seed, không sửa ngày checkpoint để vượt kiểm tra.
Migration dữ liệu bootstrap cụ thể chưa tự chạy trong thay đổi này.

Với một target mới hoàn toàn, có thể chủ động bật `FABRIC_ALLOW_INITIAL_PULL=true`
để lần đầu lấy full; tắt lại sau bootstrap. Không bật trên target đã có dữ liệu.
Full-pull dài hơn timeout 2h30 cần bootstrap ngoài timer rồi seed; pull đang dở
chưa được durable-resume, chỉ batch đã prepare/publish dở được tự retry.

## Build và rollout

Thay đổi này không sửa `.gitlab-ci.yml`; pipeline tiếp tục hoạt động theo cấu
hình sẵn có. Package cần cài `.[fabric]` để có deltalake/pyarrow; source runtime
nằm trong `src/app`. Nếu team muốn thêm test MR hoặc deploy production, nên thực
hiện bằng một thay đổi CI/CD riêng sau khi thống nhất quy trình.

Trước khi bật timer: seed state → gọi ingestion API có authentication cho một
vendor → đối chiếu Delta count và Blob phase committed → chạy lại để kiểm tra
incremental/no-change → lần lượt mở các vendor còn lại. Dùng API ingestion hiện
có để kiểm tra runtime incremental.

Rollback: disable timer, giữ Blob state và Delta log, deploy lại package được
duyệt. Nếu Blob phase pending, giữ nguyên batch đó để version tương thích retry.
Không restore checkpoint cũ hơn các transaction Delta đã publish.

## Kiểm chứng và giới hạn

Kiểm tra local 2026-09-13: **266 tests pass**, Ruff sạch trên toàn bộ backend,
strict mypy pass trên 102 source files. Build wheel thành công, kiểm tra có đủ module runtime, không
đóng gói thư mục data/scripts và kiểm tra cú pháp Python 3.11. Đây không phải kết
quả chạy pipeline GitLab hoặc deploy Linux thực tế.

Đã kiểm tra local bằng Delta thật và Azure state giả lập: append Bronze, restore
chính xác committed Delta version, schema bổ sung, checksum/count, retry sau
commit/commit-state failure, process restart, reject partial failure và không tự
full-resync qua tháng mới.
Chưa xác minh end-to-end bằng chính Managed Identity, đường mạng, seed bootstrap
production hoặc Linux deployment trong môi trường của công ty. Các test mock không chứng
minh response/filter của vendor production luôn đúng như mock.

Tài liệu tham chiếu:

- [Microsoft: OneLake Python Delta access](https://learn.microsoft.com/en-us/fabric/onelake/onelake-azure-databricks)
- [Microsoft: OneLake API endpoints and authentication](https://learn.microsoft.com/en-us/fabric/onelake/onelake-access-api)
- [Delta-rs transaction API](https://delta-io.github.io/delta-rs/api/transaction/)
