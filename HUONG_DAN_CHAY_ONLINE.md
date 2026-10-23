# 🌐 Hướng Dẫn Chạy Tool Online Để Truy Cập Ở Bất Cứ Đâu

Có **3 cách đơn giản** để bạn có thể mở và sử dụng công cụ này từ điện thoại (iPhone / Android) hoặc laptop ở bất kỳ nơi nào (ngoài đường, quán cà phê, công ty):

---

## 🚀 Cách 1: Triển khai Miễn Phí 24/7 lên Render.com (Khuyên dùng nhất ⭐⭐⭐⭐⭐)
*Ưu điểm: Hoàn toàn MIỄN PHÍ, chạy liên tục 24/7 trên đám mây, bạn có thể TẮT MÁY TÍNH ở nhà thoải mái. Render sẽ cấp 1 đường link HTTPS cố định (ví dụ: `https://binance-scanner.onrender.com`).*

### Các bước thực hiện (mất 2 phút):
1. **Tạo tài khoản:** Vào [render.com](https://render.com) và đăng ký tài khoản miễn phí (bằng GitHub hoặc Email).
2. **Đưa code lên GitHub:**
   - Tạo 1 repository mới trên GitHub (chế độ Public hoặc Private đều được).
   - Tải toàn bộ các file trong thư mục `binance-futures-scanner` lên repository này (đã có sẵn file `Dockerfile`, `Procfile`, `requirements.txt`).
3. **Tạo Web Service trên Render:**
   - Trên Dashboard của Render, bấm nút **"New +"** ➡️ Chọn **"Web Service"**.
   - Kết nối với repository GitHub bạn vừa tạo.
   - Các mục cài đặt:
     - **Name:** Đặt tên bạn thích (ví dụ `binance-scanner`).
     - **Language:** Chọn `Python`.
     - **Build Command:** Để trống (hoặc `pip install -r requirements.txt`).
     - **Start Command:** `python scanner.py`
     - **Instance Type:** Chọn gói `Free` ($0/tháng).
   - Bấm **"Deploy Web Service"**.
4. **Sử dụng:**
   - Đợi khoảng 1-2 phút Render khởi động xong, bạn sẽ nhận được đường link HTTPS (ví dụ: `https://binance-scanner-xyz.onrender.com`).
   - Bạn chỉ cần lưu link này vào Bookmark trên trình duyệt điện thoại là mở ra soi chart bất cứ lúc nào!

---

## ⚡ Cách 2: Mở Link Truy Cập Điện Thoại Qua Cloudflare Tunnel (Nhanh nhất - 30 giây)
*Ưu điểm: Không cần tài khoản GitHub, không cần deploy. Dùng khi bạn đang bật máy tính ở nhà và muốn soi trên điện thoại ngay lập tức.*

1. Tải công cụ miễn phí `cloudflared` của Cloudflare:
   - Tải file `cloudflared-windows-amd64.exe` từ trang chủ Cloudflare và đổi tên thành `cloudflared.exe`.
2. Mở Terminal / CMD và gõ lệnh:
   ```bash
   cloudflared tunnel --url http://127.0.0.1:5000
   ```
3. Cloudflare sẽ tạo ra ngay 1 đường link dạng:
   👉 `https://xxxx-xxxx-xxxx.trycloudflare.com`
4. Bạn copy link đó gửi vào Zalo / Telegram để mở trên điện thoại!

---

## 🖥️ Cách 3: Chạy trên VPS (Cloud Server)
Nếu bạn có VPS Ubuntu / Debian (như Oracle Cloud Free, Google Cloud, DigitalOcean):
```bash
# Clone hoặc copy thư mục vào VPS
cd binance-futures-scanner
# Chạy ngầm 24/7
nohup python3 scanner.py > scanner.log 2>&1 &
```
Truy cập qua địa chỉ: `http://<IP_VPS>:5000`.
