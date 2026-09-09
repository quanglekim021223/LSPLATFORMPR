# Demo local: FHU → akajob → SkillUp → Bronze

## Phạm vi và bằng chứng

Demo chạy logic BE qua HTTP thật tới server giả lập, không gọi FHU, akajob hoặc
iMocha thật. Không thay đổi tám vendor hiện tại, scheduler, Function App hoặc `.env`.
Không tự tạo employee trên SkillUp. Chỉ dùng dữ liệu nhân viên giả `example.test`.

Luồng:

1. GET `/fhu/employees`: nguồn employee giả lập.
2. GET `/akajob/certificates`: nguồn certificate/skill code giả lập.
3. Kiểm tra employee, mapping và certificate; dừng trước khi POST nếu dữ liệu không hợp lệ.
4. POST `/skillup/certificates/create`: tạo certificate cùng taxonomy skills.
5. POST `/skillup/employees/{employeeId}/certificates`: gửi mảng certificate của employee.
6. Poll GET `/skillup/employees/skills-profile` đến khi thấy các skill mong đợi,
   hoặc báo lỗi khi hết số lượt chờ. HTTP 201 không được coi là xử lý xong.
7. Lưu nguyên response JSON từng lần gọi vào thư mục Bronze riêng theo run ID.

`/skillup` là prefix của mock server, không phải prefix đã xác nhận của API thật.
`/demo/info` chỉ để nhận diện simulator; không phải API processing status của iMocha.

## Cấu trúc code dùng chung

Logic nghiệp vụ nằm trong code chính của backend, không có package `app/demos`:

- `schemas/fhu/responses.py`, `schemas/akajob/responses.py`: contract nguồn tạm thời.
- `schemas/skillup/certificates.py`: contract request SkillUp và kiểu mapping skill.
- `clients/fhu_client.py`, `clients/akajob_client.py`: gọi và đọc response từng nguồn.
- `clients/skillup_certificate_client.py`: hai POST và GET profile; không retry POST.
- `services/skillup/certificate_exchange.py`: mapping, kiểm tra, điều phối, chờ kết quả,
  lưu raw qua `BronzeWriter` hiện có. Nhận client, mapping và writer từ caller, không
  import mock hay chứa URL/token giả lập. Không phụ thuộc local filesystem.
- `mocks/learning_exchange.py`, `mocks/learning_exchange_data.py`,
  `mocks/fixtures/learning_sources.json`: server giả lập, quy tắc và dữ liệu demo.
- `commands/learning_exchange.py`: entrypoint local ghép client với mock và gọi chính
  service ở trên. Chỉ lớp entrypoint này có kiểm tra loopback/marker và key demo.

Khi có API thật, cập nhật source adapter/schema và mapping được cung cấp cho service.
Không cần viết lại luồng chỉ vì thay mock bằng HTTP thật. Những phần nghiệp vụ chưa
xác nhận như match certificate, pagination hoặc xử lý bất đồng bộ vẫn có thể cần sửa.
Chưa đăng ký endpoint ghi dữ liệu hoặc schedule production cho tính năng mới.

## Contract đã thấy trong tài liệu

Dựa trên ảnh tài liệu do người dùng cung cấp:

- [Create certificate with skills](https://developer.imocha.io/create-certificate-with-skills-42162867e0):
  POST `/certificates/create`, header `x-api-key`, body object gồm `title`,
  `certificateIssuer`, `skills: [{taxonomySkillId: integer}]`; tất cả bắt buộc.
  HTTP 200 trả object với cùng nhóm field; không có `certificateId` trong schema ảnh.
  Endpoint được gắn nhãn **Developing**.
- [Upload certificate for employee](https://developer.imocha.io/upload-certificate-for-employee-27048385e0):
  POST `/employees/{employeeId}/certificates`, header `x-api-key`, body **array**.
  Mỗi phần tử bắt buộc `title`, `isStandardCertificate`; optional `issuer`,
  `validTill`, `licenseNumber`. HTTP 201 trả object, ví dụ `{}`.
  Path chấp nhận Employee External ID theo ảnh tài liệu.

Readback dùng endpoint và validator Skill Inventory đã có trong repository.
Điều này không xác nhận live contract trên tenant iMocha hiện tại.

## Giả định chỉ dùng cho demo

- FHU sở hữu danh sách employee; akajob cung cấp certificate cùng `skillCodes`.
  Đây là cách phân vai giả định, không phải contract FHU/akajob đã được duyệt.
- API nguồn trả một trang `items`; chưa có pagination, watermark hoặc xác thực thật.
- Employee trong file nguồn lúc **khởi động mock** đã tồn tại trên mock SkillUp.
  Thêm employee mới trong file sau đó không tự enroll họ; upload sẽ trả 404.
- Mapping trong `app/mocks/learning_exchange_data.py` khai báo tường minh source
  skill code → taxonomySkillId → skillId/name trả về. Không coi hai loại ID là một.
- Mock liên kết certificate bằng cặp **title + issuer chính xác**. `certificateIssuer`
  của API tạo tương ứng `issuer` của API upload trong demo.
- Demo chỉ hỗ trợ `isStandardCertificate=true`. Ý nghĩa đầy đủ của flag chưa xác nhận.
- Tạo lại cùng title/issuer cập nhật một entry; upload lại cùng employee/title/issuer
  cập nhật một entry. Đây là dedup của mock, không phải cam kết idempotency từ iMocha.
- Mock xuất hiện skills sau một khoảng trễ, không chạy AI inference thật. Các score
  trong hồ sơ chỉ là giá trị giả để đáp ứng read contract, không chứng minh proficiency.
- Skill đã có từ trước có thể thỏa readback. Đây là kiểm tra skill hiện diện, không
  chứng minh version chứng chỉ mới nhất đã được xử lý.
- Không giải quyết revoke/delete certificate hoặc skill, lịch sử nhiều chứng chỉ cùng
  title/issuer, expiry, bulk production, crash recovery hay exactly-once delivery.

## Chạy demo

Từ repository root, dùng virtualenv đã cài backend dependencies theo README chính.
Không dùng API key thật. Server và runner dùng chung key công khai chỉ dành cho demo.

Terminal 1:

```bash
.venv/bin/python -m uvicorn app.mocks.learning_exchange:app --host 127.0.0.1 --port 9100
```

Swagger: `http://127.0.0.1:9100/docs`.

Terminal 2:

```bash
.venv/bin/python -m app.commands.learning_exchange
```

Runner chỉ chấp nhận IP loopback qua HTTP, kiểm tra marker simulator, không đi theo
redirect. CLI bỏ qua proxy môi trường và không đọc `.env` hay key production.
Không bind server ra `0.0.0.0`; simulator không được thiết kế để public.

Kết quả thành công có `mode: mock-only`, `status: succeeded`, run ID, số employee,
certificate, số lượt poll và đường dẫn lưu dữ liệu. Lỗi trả exit code khác 0.
Trong các response raw của `skill_inventory`, so sánh lần đầu chưa có skill với lần
sau có Python và SQL khi chạy trên mock mới khởi động.

Dữ liệu mặc định lưu riêng tại `data/learning-exchange-demo/`:

- `fhu/employees/.../run_id=.../offset=000001.json`
- `akajob/certificates/.../run_id=.../offset=000001.json`
- `skillup/certificate_catalog_responses/...`
- `skillup/employee_certificate_responses/...`
- `skillup/skill_inventory/...` chứa từng trang của từng lượt poll.

Mỗi thư mục có manifest/hash từ LocalBronzeWriter hiện có. Đây là local Bronze,
không phải OneLake thật. Không đăng ký run demo vào SQLite/coordinator hiện có và
không thêm FHU/akajob vào dashboard API; vì vậy xem kết quả qua CLI và file raw.

## Thử sửa nguồn và chạy lại

File mẫu: `backend/src/app/mocks/fixtures/learning_sources.json`.
Nên copy ra file thử nghiệm thay vì sửa fixture đã commit:

```bash
mkdir -p backend/data
cp backend/src/app/mocks/fixtures/learning_sources.json backend/data/demo-learning-sources.json
LEARNING_EXCHANGE_DEMO_SOURCE_FILE="$PWD/backend/data/demo-learning-sources.json" \
  .venv/bin/python -m uvicorn app.mocks.learning_exchange:app --host 127.0.0.1 --port 9100
```

Dừng server cũ trước nếu đang dùng port 9100. Trong file copy, sửa `licenseNumber`
hoặc thêm certificate với `awardId`/`title` mới cho employee hiện tại; skill code hỗ
trợ là `python`, `sql`. Chạy lại runner. GET nguồn đọc file mỗi lần nên không cần
restart để nhận thay đổi. Mỗi lượt có raw snapshot riêng; mock không tăng số entry
khi gửi lại cùng danh tính certificate. Server restart sẽ mất state mock.

Để mô phỏng xử lý lâu, khởi động server với
`LEARNING_EXCHANGE_DEMO_DELAY_SECONDS=60`, rồi chạy runner:

```bash
.venv/bin/python -m app.commands.learning_exchange --max-polls 2 --poll-interval 0.1
```

Runner phải báo chưa thấy skills, không báo thành công chỉ vì upload trả 201.
POST lỗi hoặc mất kết nối không được retry tự động. Nếu lỗi sau một số POST thành
công, có thể còn state đã ghi trên mock và raw files của lượt lỗi. Kiểm tra trước
khi chạy lại; demo không có transaction xuyên hệ thống hay outbox bền vững.

## Kiểm thử và điều kiện chuyển sang API thật

```bash
.venv/bin/python -m pytest -q backend/tests/integration/mocks/test_learning_exchange.py
```

Tests gọi HTTP bằng ASGI transport với simulator thật, kiểm tra contract, thứ tự
request, raw persistence, dữ liệu sửa/thêm, rerun, auth, thiếu mapping/employee,
polling timeout, POST không retry và chặn origin không phải loopback.

Trước production cần xác nhận: schema/auth/pagination FHU và akajob, employee
provisioning/identity, mapping taxonomy, cách match certificate, semantics flag,
idempotency/update/revoke, giới hạn API, thời điểm readback và quyền tenant.
Không chỉ đổi base URL rồi dùng runner demo để gọi production.
