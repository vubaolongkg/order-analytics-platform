import json
import os
import random
import time
import uuid
from faker import Faker
from confluent_kafka import Producer

fake = Faker('vi_VN')

# Kết nối qua external port của Redpanda
KAFKA_BROKER = "localhost:19092"
TOPIC_NAME = "food-orders"

producer = Producer({
    'bootstrap.servers': KAFKA_BROKER,
    'client.id': 'order-simulator'
})

STATUS_SEQUENCE = ['CREATED', 'ASSIGNED', 'PICKED_UP', 'DELIVERED']

# Tải danh sách quán ăn đã cào
restaurants_file = "ingestion/data/restaurants.json"
if not os.path.exists(restaurants_file):
    raise FileNotFoundError("Chưa tìm thấy ingestion/data/restaurants.json. Hãy chạy scraper.py trước!")

with open(restaurants_file, "r", encoding="utf-8") as f:
    RESTAURANTS = json.load(f)

def delivery_report(err, msg):
    if err is not None:
        print(f"Message delivery failed: {err}")

def create_initial_order():
    restaurant = random.choice(RESTAURANTS)
    # Chọn ngẫu nhiên 1 - 3 món ăn thật từ quán
    num_items = random.randint(1, 3)
    selected_items = random.sample(restaurant['menu'], min(num_items, len(restaurant['menu'])))
    
    total_amount = sum(item['price'] for item in selected_items)
    order_id = str(uuid.uuid4())
    
    # Sinh tọa độ giao hàng quanh quán (bán kính ~1-3 km)
    customer_lat = restaurant['lat'] + random.uniform(-0.02, 0.02)
    customer_lon = restaurant['lon'] + random.uniform(-0.02, 0.02)

    return {
        "order_id": order_id,
        "step_idx": 0,
        "restaurant_id": restaurant['merchant_id'],
        "restaurant_name": restaurant['name'],
        "restaurant_category": restaurant['category'],
        "restaurant_lat": restaurant['lat'],
        "restaurant_lon": restaurant['lon'],
        "customer_id": fake.uuid4(),
        "customer_lat": round(customer_lat, 6),
        "customer_lon": round(customer_lon, 6),
        "driver_id": None,
        "items": [item['item_name'] for item in selected_items],
        "total_amount": float(total_amount),
    }

def main():
    print(f"Bắt đầu đẩy event đơn hàng vào topic [{TOPIC_NAME}] qua {KAFKA_BROKER}...")
    active_orders = []

    try:
        while True:
            # 1. Quyết định tạo đơn mới hay cập nhật trạng thái đơn cũ
            if len(active_orders) < 25 or random.random() < 0.45:
                order = create_initial_order()
                status = "CREATED"
                active_orders.append(order)
            else:
                idx = random.randint(0, len(active_orders) - 1)
                order = active_orders[idx]
                order['step_idx'] += 1

                # 5% xác suất đơn bị hủy khi đang gán tài xế
                if order['step_idx'] == 1 and random.random() < 0.05:
                    status = "CANCELLED"
                    active_orders.pop(idx)
                elif order['step_idx'] < len(STATUS_SEQUENCE):
                    status = STATUS_SEQUENCE[order['step_idx']]
                    if status == "ASSIGNED":
                        order['driver_id'] = f"DRV_{random.randint(1000, 9999)}"
                    if status == "DELIVERED":
                        active_orders.pop(idx)
                else:
                    status = "DELIVERED"
                    active_orders.pop(idx)

            # 2. Đóng gói message event
            event_payload = {
                "order_id": order['order_id'],
                "status": status,
                "restaurant_id": order['restaurant_id'],
                "restaurant_name": order['restaurant_name'],
                "restaurant_category": order['restaurant_category'],
                "restaurant_lat": order['restaurant_lat'],
                "restaurant_lon": order['restaurant_lon'],
                "customer_id": order['customer_id'],
                "customer_lat": order['customer_lat'],
                "customer_lon": order['customer_lon'],
                "driver_id": order['driver_id'],
                "items": order['items'],
                "total_amount": order['total_amount'],
                "event_timestamp": int(time.time() * 1000)
            }

            producer.produce(
                topic=TOPIC_NAME,
                key=order['order_id'].encode('utf-8'),
                value=json.dumps(event_payload, ensure_ascii=False).encode('utf-8'),
                callback=delivery_report
            )
            producer.poll(0)
            print(f"[{status}] Order: {order['order_id'][:8]}... | {order['restaurant_name']} | {order['total_amount']:,.0f} VND")

            time.sleep(random.uniform(0.3, 0.8))

    except KeyboardInterrupt:
        print("\nĐang dừng producer...")
    finally:
        producer.flush()
        print("Hoàn tất dọn buffer producer.")

if __name__ == "__main__":
    main()