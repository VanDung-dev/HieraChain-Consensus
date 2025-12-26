# HieraChain Consensus

![Python Versions](https://img.shields.io/badge/python-3.10%20|%203.11%20|%203.12%20|%203.13-blue)
[![License](https://img.shields.io/badge/license-Apache%202.0-blue.svg)](LICENSE-APACHE)
[![License](https://img.shields.io/badge/license-MIT-blue.svg)](LICENSE-MIT)

[English](README.md) | **Tiếng Việt**

## Tổng quan

HieraChain Consensus là một thư viện đồng thuận blockchain hiệu năng cao dựa trên nền tảng Rust, được thiết kế cho các ứng dụng blockchain doanh nghiệp và liên hợp (consortium). Thư viện cung cấp nhiều cơ chế đồng thuận, bảo mật mật mã, giảm thiểu lỗi và tích hợp liền mạch với Python thông qua các binding PyO3.

Kho lưu trữ này chứa thành phần đồng thuận của [HieraChain](https://github.com/VanDung-dev/HieraChain).

## Tính năng

### Chức năng cốt lõi

- **Đa cơ chế đồng thuận**:
  - **Proof of Federation (PoF)**: Đồng thuận luân phiên leader kiểu round-robin cho các blockchain liên hợp.
  - **Byzantine Fault Tolerance (BFT)**: Giao thức cam kết 3 pha (pre-prepare, prepare, commit) cho các ứng dụng doanh nghiệp.
  - **Proof of Authority (PoA)**: Đồng thuận dựa trên ủy quyền cho các mạng lưới tin cậy.

- **Quản lý Block hiệu năng cao**:
  - Tạo block hiệu quả với xác minh cây Merkle.
  - Tích hợp Apache Arrow để truyền dữ liệu zero-copy giữa Rust và Python.
  - Các thao tác xử lý theo lô (batch) để giảm thiểu chi phí FFI.

- **Dịch vụ Sắp xếp (Ordering Service)**:
  - Dịch vụ sắp xếp đa node với bầu chọn leader.
  - Hàng đợi sự kiện và xử lý với các quy tắc xác thực có thể cấu hình.
  - Giám sát trạng thái thời gian thực và kiểm tra sức khỏe hệ thống.

- **Bảo mật mật mã**:
  - Tạo và quản lý cặp khóa Ed25519.
  - Tạo và xác minh chữ ký số.
  - Ký tin nhắn an toàn cho các giao thức đồng thuận.

- **Giảm thiểu lỗi**:
  - Phân loại lỗi và quản lý mức độ ưu tiên.
  - Xác thực đồng thuận với các ngưỡng có thể cấu hình.
  - Cơ chế phục hồi và nhật ký kiểm toán (audit journaling).

### Điểm nổi bật về kỹ thuật

- **Triển khai bằng Rust**: Core hiệu năng cao, an toàn bộ nhớ viết bằng Rust.
- **Python Bindings**: Tích hợp liền mạch với các ứng dụng Python sử dụng PyO3.
- **Tích hợp Arrow**: Tương tác zero-copy với PyArrow để xử lý dữ liệu hiệu quả.
- **Async Runtime**: Các hoạt động bất đồng bộ dựa trên Tokio cho đồng thuận BFT.
- **Kiến trúc mô-đun**: Phân tách rõ ràng các mối quan tâm giữa các mô-đun đồng thuận, bảo mật và xử lý lỗi.

## Bắt đầu nhanh

### Cài đặt

```bash
# Cài đặt từ mã nguồn
pip install maturin
maturin develop
```

### Sử dụng cơ bản

```python
from hierachain_consensus import Block, ProofOfFederation, KeyPair

# Tạo một block
block = Block(
    index=1,
    events=[{"type": "transfer", "from": "Alice", "to": "Bob"}],
    previous_hash="0" * 64
)

# Khởi tạo đồng thuận
pof = ProofOfFederation(name="Consortium")
pof.add_validator("validator-1")
pof.add_validator("validator-2")

# Các thao tác mật mã
keypair = KeyPair.generate()
signature = keypair.sign(b"message")
```

## Tổng quan kiến trúc

HieraChain Consensus được xây dựng với kiến trúc mô-đun phân tách các mối quan tâm qua nhiều lớp:

- **Lớp Đồng thuận**: Đa cơ chế đồng thuận (PoF, BFT, PoA) với dịch vụ sắp xếp.
- **Lớp Core**: Quản lý block, cây Merkle và các tiện ích mật mã.
- **Lớp Bảo mật**: Quản lý khóa Ed25519 và xác minh chữ ký.
- **Lớp Giảm thiểu lỗi**: Phân loại lỗi, xác thực và các cơ chế phục hồi.

### Quy trình đồng thuận

1. **Gửi sự kiện** → Các sự kiện được gửi đến Dịch vụ Sắp xếp.
2. **Xác thực** → Các sự kiện được xác thực dựa trên schema và quy tắc nghiệp vụ.
3. **Sắp xếp** → Các sự kiện được sắp xếp bởi cơ chế đồng thuận (PoF/BFT).
4. **Tạo Block** → Các sự kiện đã sắp xếp được đóng gói thành các block.
5. **Xác minh** → Các block được xác minh (Merkle root, chữ ký).
6. **Cam kết** → Các block được cam kết vào blockchain.

### Giao thức BFT 3 Pha

1. **Pre-Prepare**: Primary phát đề xuất đến tất cả các bản sao (replica).
2. **Prepare**: Các replica xác thực và phát tin nhắn prepare.
3. **Commit**: Sau khi nhận đủ 2f+1 prepare, các replica phát tin nhắn commit.
4. **Execute**: Sau khi nhận đủ 2f+1 commit, thao tác được thực thi.

## Điểm nổi bật về hiệu năng

- **Truyền dữ liệu Zero-Copy**: Apache Arrow giúp trao đổi dữ liệu Python ↔ Rust hiệu quả.
- **Thao tác Batch**: Giảm chi phí FFI lên đến 10 lần.
- **Async I/O**: Runtime Tokio cho các hoạt động đồng thời hiệu quả.
- **Cây Merkle tối ưu hóa**: Xây dựng và xác minh cây hiệu quả.

## Giấy phép

Dự án này được cấp phép kép dưới [Giấy phép Apache-2.0](LICENSE-APACHE) hoặc [Giấy phép MIT](LICENSE-MIT). Bạn có thể chọn một trong hai giấy phép.

---
