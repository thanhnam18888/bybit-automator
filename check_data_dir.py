import os

FOLDER = "/data/bybit_full_history"

print("🔍 Đang kiểm tra thư mục:", FOLDER)

if not os.path.exists(FOLDER):
    print("❌ Thư mục KHÔNG tồn tại.")
else:
    files = os.listdir(FOLDER)
    print(f"✅ Thư mục tồn tại. Tổng số file: {len(files)}")
    if files:
        print("📂 Một vài file đầu tiên:")
        for f in files[:10]:
            print(" -", f)
    else:
        print("⚠️ Thư mục có tồn tại nhưng RỖNG.")
