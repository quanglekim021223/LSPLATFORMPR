# Fabric Bronze ingestion handoff

Cập nhật: 2026-09-11  
Repository: `/Users/quangkimle/Documents/code/LSPLATFORMPR`  
Backend: `/Users/quangkimle/Documents/code/LSPLATFORMPR/backend`  
Nhánh hiện tại: `feat/fabric_onelake`

## Đọc phần này trước khi tiếp tục

### Bổ sung runtime production (2026-09-11)

Đã thêm `fabric_job.py`, `fabric_state.py`, `fabric_tables.py`, `fabric_seed.py`
và nối timer/API khi `FABRIC_ENABLED=true`. Runtime dùng Managed Identity,
leased Blob checkpoint, prepared Parquet batch và Delta transaction retry.
Các gap code liệt kê trong phần cũ bên dưới cần đối chiếu với
[fabric-production.md](fabric-production.md); đó là tài liệu rollout hiện tại.
Nội dung MR nằm ở [fabric-production-mr.md](fabric-production-mr.md).
Chưa deploy, chưa seed checkpoint production và chưa chạy smoke test live.
Kiểm chứng local: 261 tests pass, Ruff sạch trên toàn bộ backend, mypy pass trên
101 source files;
wheel build và kiểm tra nội dung thành công. Đã test cửa sổ SkillUp bị truncate,
checkpoint seed thiếu scope, publish retry và commit-state failure.
Chưa commit/push hoặc tạo MR remote. Remote hiện tại là GitHub
`quanglekim021223/LSPLATFORMPR`; cần xác nhận repository GitLab đích nếu tạo MR
trên hệ thống công ty. Giữ timer disabled đến khi target/RBAC/state/smoke đã ổn.

Mục tiêu là chạy ingestion theo lịch trên Azure Function App:

```text
Timer
→ pull phần dữ liệu mới theo watermark/cửa sổ overlap
→ xử lý JSON/CSV tạm trong runtime
→ chuyển mỗi endpoint thành một Delta table Bronze
→ cập nhật table trong Fabric Lakehouse
→ chỉ cập nhật checkpoint khi cả pull và publish thành công
```

Không được quay lại flow upload raw JSON/CSV vào `Files/vendor_raw` trên Fabric.
Raw chỉ là dữ liệu trung gian; Fabric Bronze phải chứa table trong `Tables/dbo`.

## Quyết định Bronze đã chốt

- Mỗi endpoint vendor trở thành một table; FAMS tạo hai table từ `classList` và
  `studentList`.
- Không chuẩn hóa nghiệp vụ, không deduplicate business record và không tách mảng
  thành bảng con. Những việc đó thuộc Silver.
- Top-level field trở thành column `lower_snake_case` an toàn cho SQL.
- Tất cả physical columns là `STRING`.
- Array/object nested được giữ dưới dạng JSON string trong một cell.
- Mỗi bảng endpoint có thêm bốn cột kỹ thuật: `_ingested_at`, `_run_id`,
  `_source_vendor`, `_source_domain`. Bronze không tạo thêm bảng audit, reject
  hoặc checkpoint; checkpoint runtime vẫn nằm trong Blob state riêng.
- Những row đã publish trước contract này không được tự backfill; cần target trống
  hoặc một lần rebuild/bootstrap được duyệt để metadata của dữ liệu cũ không NULL.
- `skillup_certificates` và `skillup_learning_resources` là hai bảng nghiệp vụ
  chính thức và được runtime tự động cập nhật cùng ba bảng SkillUp còn lại. Hai
  bảng giữ schema flatten đã bootstrap để lần append sau tương thích bảng hiện có.

## Fabric target

- Workspace ID: `851dacd8-2ad0-42a1-8817-55d2a7682bc6`
- Lakehouse ID: `3535e0e3-a2a0-4387-82e6-1e7d8cbd1a62`
- SQL Endpoint ID: `f2cc4fd8-68d5-41fd-b09b-136b5765d1da`
- Lakehouse/SQL Endpoint name: `lh_bronze_dev`
- Schema: `dbo`
- OneLake account URL:
  `https://southeastasia-onelake.dfs.fabric.microsoft.com`
- Table destination:
  `3535e0e3-a2a0-4387-82e6-1e7d8cbd1a62/Tables/dbo/<table>`

SQL Endpoint là lớp đọc metadata của Delta tables trong Lakehouse. Đích ghi là
Lakehouse `Tables/dbo`, không phải SQL Endpoint và không phải `Files`.

## Bootstrap đã hoàn tất

Batch bootstrap lịch sử đã pull 17 dataset contracts và tạo 18 Delta tables.
Hai bảng SkillUp Learning Resources/Certificates đã được nạp ở đợt trước, nên
OneLake hiện có 20 bảng. Runtime mới quản lý 20 dataset và 21 bảng; bảng
`coursera_course_detail` sẽ được tạo ở lần Coursera pull đầu tiên sau rollout.
Vì checkpoint hiện tại chưa có watermark riêng cho domain này, lần pull đó sẽ
quét full Coursera catalog để backfill toàn bộ Course Detail; các lần sau chỉ lấy
catalog/detail mới hoặc vừa thay đổi theo `modifiedSinceTimestamp`.
FAMS đã được chạy qua mạng công ty.

Nguồn chính xác của từng table và manifest nằm trong:

- `data/fabric_tables/bronze_bootstrap/build_report.json`
- `data/fabric_tables/bronze_bootstrap/publish_report.json`
- `data/fabric_runs/*/report.json`

Kết quả bootstrap:

| Table | Rows |
|---|---:|
| `levelup_course_catalog` | 2,215 |
| `levelup_learning_history` | 1,032,549 |
| `skillup_skill_taxonomy` | 17,681 |
| `skillup_skill_inventory` | 42,153 |
| `skillup_assessment_history` | 452,800 |
| `datacamp_course_catalog_live` | 764 |
| `datacamp_course_catalog_archived` | 129 |
| `datacamp_learning_history` | 106,386 |
| `coursera_course_catalog` | 28,242 |
| `coursera_learning_history` | 106,189 |
| `linkedin_course_catalog` | 22,478 |
| `linkedin_learning_history` | 458,725 |
| `harvard_hmm_course_catalog` | 42 |
| `harvard_hmm_learning_history` | 84,523 |
| `harvard_spark_course_catalog` | 29,568 |
| `harvard_spark_learning_history` | 1,963,561 |
| `fams_training_classes` | 957 |
| `fams_training_students` | 22,378 |

Tổng: **18 tables, 4,371,340 rows**.

Publish report bootstrap hoàn tất với 14 table `published` và 4 table
`already_published`. Cộng với hai bảng SkillUp đã nạp trước đó, OneLake có tổng
cộng 20 table trong `dbo`. Runtime mới tiếp tục cập nhật 20 bảng này và tạo thêm
`coursera_course_detail` ở lần Coursera pull thành công đầu tiên.

## Thành phần Fabric đang giữ

- `src/app/fabric_runtime.py`: chỉ map/validate Function App settings cho local
  bootstrap; không còn OneLake raw writer.
- `src/app/fabric_catalog.py`: pagination kiểm soát Coursera/LinkedIn catalog.
- `src/app/fabric_contract.py`: mapping 20 dataset contracts → 21 table names và
  record paths.
- `tests/unit/test_fabric_ingestion.py` và
  `tests/unit/test_fabric_job.py`: kiểm tra contract và runtime Fabric.

Các script lấy sample, Notebook raw loader, `fabric_load.py` và plan cũ đã bị xóa
vì được full-pull thay thế hoặc trái với flow hiện tại. `outputs/` đã được thêm vào
`.gitignore`; file Excel/inspection vẫn còn trên máy.

## Sửa lỗi vendor đã thực hiện

- LevelUP `_offset` là zero-based page index, không phải row offset.
- Resume LevelUP enrollment không áp watermark của lần chạy trước vào giữa một
  snapshot đang phân trang.
- HTTP 401 sau khi refresh token thất bại được coi là retryable ở cấp job.
- SkillUp Taxonomy giới hạn tối đa 50 records/page trong bootstrap live.
- Các nullable/shape khác response thật đã được chấp nhận cho LevelUP, SkillUp,
  DataCamp, Coursera, LinkedIn, Harvard và FAMS.
- Harvard HMM chấp nhận header CSV `Product ID`.
- Empty source file hợp lệ nếu record count bằng 0.

Mock LevelUP cũng đã đổi sang page-index pagination. Trạng thái kiểm tra sau khi
dọn file: **268 tests passed**. Ruff cho các file thay đổi/giữ lại pass; strict
mypy cho ba Fabric bootstrap modules pass.

## Trạng thái production hiện tại

Các gap code cũ về Managed Identity, Delta append, Blob checkpoint, pending retry
và kiểm tra `partial_failure` đã được implement. Phần còn lại là rollout thật:

1. Xác nhận production Workspace/Lakehouse/Blob state target; không dùng nhầm DEV.
2. Cấp quyền Managed Identity cho OneLake và Blob state.
3. Đối soát checkpoint bootstrap đủ mọi domain rồi seed bằng `fabric_seed.py`.
4. Xác minh network/allowlist từ Function App tới FAMS và Harvard SFTP.
5. Deploy với timer disabled, smoke-test một vendor qua HTTP hai lần và đối chiếu
   Delta row count cùng Blob phase `committed`.
6. Mở lần lượt vendor còn lại; chỉ bật timer khi tất cả smoke test đạt.

## Incremental behavior cần giữ

“Chỉ pull mới nhất” không giống nhau ở mọi vendor:

- LevelUP, SkillUp, Coursera và LinkedIn dùng watermark/filter khi endpoint hỗ trợ.
- Learning history dùng overlap/lookback để không bỏ sót late-arriving records;
  vì vậy có thể đọc lại một khoảng nhỏ, nhưng không full-pull lịch sử.
- Harvard catalog dùng start-date overlap; Harvard SFTP chỉ lấy source file chưa
  xử lý.
- DataCamp catalog và FAMS phải gọi snapshot endpoint rồi so content fingerprint;
  nếu không đổi thì không ghi batch mới.

Không bỏ overlap chỉ để tránh đọc lại record: việc loại trùng nghiệp vụ thuộc
Silver, còn idempotency kỹ thuật của retry vẫn phải được bảo đảm ở Bronze.

## Việc tiếp theo

1. Review và commit riêng nhóm vendor fixes, local/bootstrap tools, production
   runtime và tests/docs để MR dễ đọc.
2. Xác nhận remote/repository đích rồi push và tạo MR.
3. Sau khi MR được duyệt, thực hiện sáu bước rollout ở phần trên; không bật timer
   ngay sau deploy.

## Lệnh kiểm tra nhanh

Chạy từ `backend/`:

```bash
../.venv/bin/python -m pytest -q
../.venv/bin/ruff check src/app/fabric_catalog.py src/app/fabric_contract.py \
  src/app/fabric_runtime.py
../.venv/bin/mypy src/app/fabric_catalog.py \
  src/app/fabric_contract.py src/app/fabric_runtime.py
git status --short
```

## Không được làm khi tiếp tục

- Không full-pull lại toàn bộ vendor chỉ để thử scheduler.
- Không upload raw JSON/CSV lên Fabric.
- Không overwrite hoặc xóa thủ công hai bảng SkillUp đã có; runtime append theo
  Delta transaction như các bảng còn lại.
- Không log credentials, access token hoặc raw PII.
- Không reset/xóa `data/fabric_runs` hoặc `data/fabric_tables` khi chưa có backup.
- Không bật timer trước khi Managed Identity, persistent checkpoint, network và
  smoke test đều đạt.
