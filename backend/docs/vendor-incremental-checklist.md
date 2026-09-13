# Checklist pull incremental cho 8 vendor

Runtime deploy: xem [fabric-production.md](fabric-production.md).
Khi `FABRIC_ENABLED=true`, checkpoint chỉ commit sau khi publish thành công;
không tự full/weekly resync định kỳ. Bronze append các phiên bản nguồn thay đổi,
không business-upsert/deduplicate. Các filter dưới đây phụ thuộc API vendor.
Nếu một table lệch khỏi Delta version đã commit, lần chạy tiếp theo tự restore
đúng version đó mà không full-pull nguồn; API trả `restored_records`. Bronze giữ
lịch sử phiên bản. Pipeline Silver riêng chịu trách nhiệm MERGE theo business key
để lấy một business record hiện tại.

Hai use case trong workbook đã có test Delta local với 50.000 record:

- Xóa một record ngoài luồng rồi chạy incremental rỗng: restore đúng record,
  không gọi full-pull và tổng row trở lại 50.000.
- Source sửa một record và thêm field: Bronze thành 50.001 row lịch sử; record mới
  có schema mới và đủ bốn metadata để Silver xử lý.

Quy tắc chung:

- [ ] Lần đầu: pull toàn bộ dữ liệu và lưu watermark/checkpoint.
- [ ] Lần sau: đọc checkpoint rồi thêm mốc thời gian vào request trước khi gọi API.
- [ ] Chỉ cập nhật checkpoint sau khi pull và lưu thành công.
- [ ] History có thể đọc lùi vài ngày để tránh bỏ sót dữ liệu đến muộn; đây không
  phải full-pull.

## 1. LevelUP

- [ ] `course_catalog`
  - Lần đầu: lấy toàn bộ course.
  - Lần sau: dùng `_filter=dateEdited gt <watermark>`.
  - Có thay đổi: chỉ nhận course mới hoặc vừa sửa.
- [ ] `learning_history`
  - Lần đầu: lấy toàn bộ enrollment của từng course.
  - Lần sau: dùng `dateEdited` watermark riêng cho từng course.
  - Có thay đổi: chỉ nhận enrollment mới hoặc vừa sửa.

## 2. SkillUp

- [ ] `skill_taxonomy`
  - Lần đầu: lấy toàn bộ taxonomy.
  - Lần sau: gửi `LastModifiedOn=<watermark>`.
  - Có thay đổi: chỉ nhận skill mới hoặc vừa sửa.
- [ ] `skill_inventory`
  - Lần đầu: lấy toàn bộ profile.
  - Lần sau: gửi `SkillProfileModifiedSince=<watermark>`.
  - Có thay đổi: chỉ nhận profile vừa sửa.
- [ ] `assessment_history`
  - Lần đầu: lấy toàn bộ khoảng lịch sử được cấu hình.
  - Lần sau: đọc từ watermark lùi 3 ngày đến hiện tại.
  - Có thay đổi: nhận report trong cửa sổ thời gian này.
- [ ] `learning_resources`
  - Lần đầu: lấy toàn bộ `/learning/materials` theo từng trang.
  - Lần sau: lấy snapshot và so fingerprint từng trang.
  - Có thay đổi: chỉ ghi những trang thay đổi; trang không đổi không được append.
- [ ] `certificates`
  - Lần đầu: lấy toàn bộ `/certificates` theo từng trang.
  - Lần sau: lấy snapshot và so fingerprint từng trang.
  - Có thay đổi: chỉ ghi những trang thay đổi; trang không đổi không được append.

## 3. DataCamp

- [ ] `course_catalog_live`
- [ ] `course_catalog_archived`
  - Lần đầu: lấy snapshot catalog.
  - Lần sau: vẫn lấy snapshot nhỏ và so fingerprint.
  - Có thay đổi: lưu snapshot mới; không đổi thì không ghi.
- [ ] `learning_history`
  - Lần đầu: lấy toàn bộ history từ ngày bắt đầu.
  - Lần sau: đọc từ watermark lùi 3 ngày.
  - Có thay đổi: nhận event trong cửa sổ này.

## 4. Coursera

- [ ] `course_catalog`
  - Lần đầu: lấy toàn bộ catalog.
  - Lần sau: gửi `modifiedSinceTimestamp=<watermark>`.
  - Có thay đổi: chỉ nhận content mới hoặc vừa sửa.
- [ ] `course_detail`
  - Nếu chưa có watermark riêng của detail: chạy full catalog một lần để backfill
    toàn bộ content ID hiện có.
  - Lấy trường `id` dạng `ContentType~Id` từ các record catalog mới hoặc vừa sửa.
  - Gọi `GET /{orgId}/contents/{id}` và ghi một row vào `coursera_course_detail`.
  - Chỉ cập nhật watermark catalog/detail sau khi tất cả detail tương ứng thành công;
    các lần sau quay lại `modifiedSinceTimestamp` incremental.
- [ ] `learning_history`
  - Lần đầu: lấy toàn bộ enrollment report.
  - Lần sau: gửi `lastActivityAfter=<watermark - overlap>`.
  - Có thay đổi: chỉ nhận activity trong khoảng mới/overlap.

## 5. LinkedIn

- [ ] `course_catalog`
  - Lần đầu: lấy toàn bộ course catalog.
  - Lần sau: gửi `lastModifiedAfter=<watermark - overlap>`.
  - Có thay đổi: chỉ nhận asset mới hoặc vừa sửa.
- [ ] `learning_history`
  - Lần đầu: lấy toàn bộ history theo các cửa sổ tối đa 14 ngày.
  - Lần sau: chỉ đọc cửa sổ từ watermark lùi overlap đến hiện tại.
  - Có thay đổi: nhận activity trong cửa sổ này.

## 6. Harvard HMM

- [ ] `course_catalog`
  - Lần đầu: lấy toàn bộ catalog, không có `startDate`.
  - Lần sau: gửi `startDate=<watermark - 1 ngày>`.
  - Có thay đổi: nhận catalog mới hoặc vừa sửa trong khoảng này.
- [ ] `learning_history`
  - Lần đầu: tải toàn bộ file lịch sử cần thiết từ SFTP.
  - Lần sau: so tên file, size và modified time với checkpoint.
  - Có thay đổi: chỉ tải file mới hoặc file đã bị sửa.

## 7. Harvard Spark

- [ ] `course_catalog`
  - Lần đầu: lấy toàn bộ catalog, không có `startDate`.
  - Lần sau: gửi `startDate=<watermark - 1 ngày>`.
  - Có thay đổi: nhận catalog mới hoặc vừa sửa trong khoảng này.
- [ ] `learning_history`
  - Lần đầu: tải toàn bộ file lịch sử cần thiết từ SFTP.
  - Lần sau: so tên file, size và modified time với checkpoint.
  - Có thay đổi: chỉ tải file mới hoặc file đã bị sửa.

## 8. FAMS

- [ ] `training_data` → `fams_training_classes` và `fams_training_students`
  - Lần đầu: lấy toàn bộ snapshot và lưu fingerprint.
  - Lần sau: endpoint vẫn trả snapshot; hệ thống so fingerprint.
  - Có thay đổi: ghi lại classes/students mới; không đổi thì không ghi.
