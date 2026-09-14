import json
import os
import requests

OVERPASS_URL = "https://overpass-api.de/api/interpreter"

# Query lấy quán ăn (restaurant, cafe, fast_food) có tên cụ thể tại khu vực trung tâm TP.HCM
QUERY = """
[out:json][timeout:25];
(
  node["amenity"~"restaurant|cafe|fast_food"]["name"](10.75,106.66,10.80,106.72);
);
out body 50;
"""

SAMPLE_MENUS = {
    "cafe": [
        {"item_name": "Cà phê sữa đá", "price": 29000},
        {"item_name": "Bạc xỉu", "price": 32000},
        {"item_name": "Trà đào cam sả", "price": 45000},
        {"item_name": "Trà sữa trân châu", "price": 39000}
    ],
    "restaurant": [
        {"item_name": "Cơm tấm sườn bì chả", "price": 55000},
        {"item_name": "Phở bò tái nạm", "price": 65000},
        {"item_name": "Bún chả Hà Nội", "price": 50000},
        {"item_name": "Gỏi cuốn tôm thịt", "price": 35000}
    ],
    "fast_food": [
        {"item_name": "Gà rán giòn cay", "price": 38000},
        {"item_name": "Burger bò phô mai", "price": 49000},
        {"item_name": "Khoai tây chiên cỡ vừa", "price": 25000},
        {"item_name": "Trà chanh sả", "price": 20000}
    ]
}

# Dataset mẫu dự phòng chuẩn vị trí trung tâm TP.HCM phòng khi máy chủ OSM quá tải
FALLBACK_RESTAURANTS = [
    {"merchant_id": "MERCH_101", "name": "Cơm Tấm Ba Ghiền", "category": "restaurant", "lat": 10.7938, "lon": 106.6698, "menu": SAMPLE_MENUS["restaurant"]},
    {"merchant_id": "MERCH_102", "name": "Phở Hòa Pasteur", "category": "restaurant", "lat": 10.7872, "lon": 106.6893, "menu": SAMPLE_MENUS["restaurant"]},
    {"merchant_id": "MERCH_103", "name": "Bánh Mì Huỳnh Hoa", "category": "fast_food", "lat": 10.7719, "lon": 106.6926, "menu": SAMPLE_MENUS["fast_food"]},
    {"merchant_id": "MERCH_104", "name": "Highlands Coffee - Hàm Nghi", "category": "cafe", "lat": 10.7715, "lon": 106.7022, "menu": SAMPLE_MENUS["cafe"]},
    {"merchant_id": "MERCH_105", "name": "The Coffee House - Trần Cao Vân", "category": "cafe", "lat": 10.7831, "lon": 106.6961, "menu": SAMPLE_MENUS["cafe"]},
    {"merchant_id": "MERCH_106", "name": "Phở Lệ - Nguyễn Trãi", "category": "restaurant", "lat": 10.7554, "lon": 106.6782, "menu": SAMPLE_MENUS["restaurant"]},
    {"merchant_id": "MERCH_107", "name": "KFC - Hai Bà Trưng", "category": "fast_food", "lat": 10.7856, "lon": 106.6953, "menu": SAMPLE_MENUS["fast_food"]},
    {"merchant_id": "MERCH_108", "name": "Pizza 4P's - Lê Thánh Tôn", "category": "restaurant", "lat": 10.7786, "lon": 106.7032, "menu": SAMPLE_MENUS["restaurant"]},
    {"merchant_id": "MERCH_109", "name": "Lotteria - Đinh Tiên Hoàng", "category": "fast_food", "lat": 10.7912, "lon": 106.6989, "menu": SAMPLE_MENUS["fast_food"]},
    {"merchant_id": "MERCH_110", "name": "Phúc Long Tea & Coffee - Ngô Đức Kế", "category": "cafe", "lat": 10.7734, "lon": 106.7048, "menu": SAMPLE_MENUS["cafe"]}
]

def fetch_real_restaurants():
    print("Đang cào dữ liệu quán ăn thực tế từ OpenStreetMap...")
    headers = {
        "User-Agent": "FoodDeliveryAnalyticsBot/1.0 (anthony.dataeng@gmail.com)",
        "Accept": "application/json"
    }
    
    restaurants = []
    try:
        response = requests.post(OVERPASS_URL, data={"data": QUERY}, headers=headers, timeout=15)
        if response.status_code == 200:
            elements = response.json().get("elements", [])
            for el in elements:
                name = el.get("tags", {}).get("name")
                amenity = el.get("tags", {}).get("amenity", "restaurant")
                lat = el.get("lat")
                lon = el.get("lon")

                if name and lat and lon:
                    menu = SAMPLE_MENUS.get(amenity, SAMPLE_MENUS["restaurant"])
                    restaurants.append({
                        "merchant_id": f"MERCH_{el['id']}",
                        "name": name,
                        "category": amenity,
                        "lat": lat,
                        "lon": lon,
                        "menu": menu
                    })
        else:
            print(f"OSM Server phản hồi status code: {response.status_code}. Kích hoạt chế độ fallback...")
    except Exception as e:
        print(f"Không thể kết nối OSM API ({e}). Kích hoạt danh sách quán ăn chuẩn...")

    # Nếu không lấy được hoặc lấy quá ít quán, nạp fallback dataset
    if len(restaurants) < 5:
        restaurants = FALLBACK_RESTAURANTS

    os.makedirs("ingestion/data", exist_ok=True)
    output_path = "ingestion/data/restaurants.json"
    with open(output_path, "w", encoding="utf-8") as f:
        json.dump(restaurants, f, ensure_ascii=False, indent=2)

    print(f"Đã lưu thành công {len(restaurants)} quán ăn vào {output_path}")

if __name__ == "__main__":
    fetch_real_restaurants()