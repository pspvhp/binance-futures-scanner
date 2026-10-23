# 🌐 Binance Futures Scanner (Chuẩn Quốc Tế UTC 00:00)

Công cụ tự động quét toàn bộ hơn 500+ cặp token Futures (USDT-M Perpetual) trên sàn Binance, tính toán giá mở cửa theo **chuẩn thị trường quốc tế (00:00:00 UTC - giờ chuẩn Binance & TradingView)**.

---

## 🌟 Giá Mở Cửa Chuẩn Quốc Tế UTC 00:00:
1. **1D (Hôm nay)**: Cây nến Ngày mở lúc **00:00:00 UTC hôm nay** (7h sáng giờ VN).
2. **1W (Tuần này)**: Cây nến Tuần mở lúc **00:00:00 UTC Thứ Hai đầu tuần**.
3. **2W (2 Tuần)**: Cây nến Tuần mở lúc **00:00:00 UTC Thứ Hai tuần trước**.
4. **1M (Tháng này)**: Cây nến Tháng mở lúc **00:00:00 UTC ngày 01 của tháng** (ví dụ: 01/10 00:00 UTC).
5. **3M (Quý)**: Cây nến 3 tháng mở lúc **00:00:00 UTC ngày 01 cách 3 tháng** (ví dụ: 01/08 00:00 UTC).
6. **6M (Nửa năm)**: Cây nến 6 tháng mở lúc **00:00:00 UTC ngày 01 cách 6 tháng** (ví dụ: 01/05 00:00 UTC).

---

## 🚀 Tính Năng Chính:
- **Đo lường khoảng cách:** Cột `🔥 Cách Open (%)` hiển thị chính xác giá hiện tại đang cao hơn (+) hoặc thấp hơn (-) bao nhiêu % so với giá Open của khung filter đang xét.
- **Sắp xếp linh hoạt:** Click vào tiêu đề bất kỳ cột nào (`Cách Open %`, `% 1D`, `% 1W`, `% 2W`, `% 1M`, `% 3M`, `% 6M`, `Volume 24h`) để sắp xếp tăng dần / giảm dần.
- **Bộ lọc đa năng:** Lọc sát giá Open (0% - 3% breakout), lọc Volume tối thiểu, tìm kiếm token nhanh.
- **Không đụng CMD:** Nhấp đúp chuột vào `run_scanner.bat` là trình duyệt tự mở.
- **Xuất File Excel (CSV):** Xuất toàn bộ thông số chuẩn quốc tế để lưu trữ và phân tích.
