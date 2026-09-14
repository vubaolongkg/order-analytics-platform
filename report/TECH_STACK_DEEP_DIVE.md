# Architecture Decisions, Trade-offs & Tech Stack Evaluation

Tài liệu này ghi lại lý do đằng sau các lựa chọn công nghệ chính trong Real-Time Food Delivery Order Analytics Platform, cùng với những đánh đổi và phương án thay thế đã cân nhắc.

---

## 1. Event streaming: Redpanda (thay vì Apache Kafka hay RabbitMQ)

Hệ thống cần nhận vài trăm sự kiện đơn hàng mỗi giây, độ trễ dưới 1 giây, và quan trọng là không ngốn hết RAM/CPU của máy dev.

Redpanda được viết bằng C++ nên không cần JVM, tránh được hiện tượng giật cục do garbage collection và đọc/ghi thẳng vào bộ nhớ. Nó tương thích gần như tuyệt đối với Kafka API, nên toàn bộ code Python (`kafka-python`) và PySpark (`spark-sql-kafka-0-10`) chạy được ngay, không cần sửa adapter. Nó cũng chỉ là một binary duy nhất, không cần Zookeeper hay KRaft, kéo RAM container từ mức 2-4GB của Kafka xuống dưới 500MB.

Đánh đổi là hệ sinh thái connector bên thứ ba (Kafka Connect) vẫn ưu tiên Kafka gốc hơn, và cộng đồng dùng Redpanda ở Việt Nam còn nhỏ so với Kafka truyền thống.

Apache Kafka là chuẩn công nghiệp nhưng tốn tài nguyên JVM quá mức cho môi trường local/test. RabbitMQ phù hợp cho task distribution hơn là mô hình log-based partition cần replay dữ liệu cho PySpark.

---

## 2. Stream processing: PySpark Structured Streaming (thay vì Apache Flink)

Bài toán ở đây là tổng hợp trạng thái theo sliding window, và cần ghi thẳng vào Delta Lake.

Structured Streaming dùng chung Catalyst Optimizer và DataFrame API với Spark SQL, nên code streaming viết gần giống hệt batch. Cơ chế micro-batch (trigger 5-10 giây) đủ tốt cho việc cập nhật GMV và tần suất đơn hàng — bài toán này không cần độ trễ microsecond. Nó cũng có sink Delta Lake gốc, hỗ trợ checkpointing để đảm bảo exactly-once.

Nhược điểm rõ nhất là độ trễ thực tế rơi vào vài chục đến vài trăm mili-giây, không xử lý event-by-event thật sự như Flink. Chi phí metadata của checkpoint file cũng phình to dần nếu không dọn dẹp (vacuum) định kỳ.

Flink xử lý streaming thật với độ trễ dưới mili-giây, nhưng vận hành cụm Flink phức tạp hơn nhiều, và việc ghi Delta Lake đòi hỏi cài thêm connector ngoài.

---

## 3. Storage: Delta Lake (thay vì Apache Iceberg hay Parquet thuần)

Cần một định dạng bảng trung gian để Spark streaming ghi nối liên tục, trong khi Airflow đọc song song mà không bị khóa file hay đọc phải dữ liệu rác.

Nhờ transaction log (`_delta_log`), Delta Lake đảm bảo ACID — writer (PySpark) và reader (Airflow/Python) không bao giờ đụng nhau. Tính năng time travel cho phép soi lại trạng thái bảng tại một mốc thời gian cụ thể, rất hữu ích lúc cần debug số liệu sai lệch. Về bản chất vẫn là Parquet nên vẫn tận dụng được columnar storage cho tốc độ đọc.

Vấn đề thường gặp là small file problem — các micro-batch streaming tạo ra rất nhiều file nhỏ, buộc phải chạy `OPTIMIZE` hoặc `VACUUM` định kỳ. Ngoài ra cần thư viện tương thích (`deltalake` trong Python, hoặc Delta Engine trong Spark) thay vì đọc trực tiếp như CSV/JSON.

Iceberg scale metadata tốt hơn cho cụm lớn, nhưng với quy mô project này, setup local với Spark của Delta Lake đơn giản hơn. Parquet thuần thì không có transaction log — đọc file trong lúc streaming đang ghi dở sẽ làm crash tiến trình downstream.

---

## 4. Orchestration: Apache Airflow (thay vì Prefect hay Cron)

Cần điều phối chu trình ETL/ELT theo lịch, quản lý phụ thuộc giữa bước đồng bộ BigQuery và bước kích hoạt dbt.

Airflow là kỹ năng được yêu cầu nhiều nhất cho vị trí Data Engineer, có UI theo dõi DAG rất trực quan — xem cây thực thi, phát hiện task lỗi, đọc log chi tiết mà không cần SSH vào container. Credentials và service account key cũng được quản lý tách biệt khỏi source code.

Bù lại, Airflow khá nặng — tối thiểu cần Postgres, Scheduler và Webserver, ngốn khoảng 1.5-2GB RAM. Scheduler polling định kỳ cũng gây độ trễ nhỏ khi trigger task.

Prefect và Dagster có thiết kế hiện đại và nhẹ hơn, nhưng độ phổ biến trong các dự án DE truyền thống chưa bằng Airflow. Cron thì quá sơ sài — không retry tự động, không có dependency graph, không quản lý metadata tập trung.

---

## 5. Data warehouse: Google BigQuery (thay vì Snowflake hay PostgreSQL local)

Cần một tầng lưu trữ phân tích tập trung, chi phí bằng 0, không phải tự bảo trì hạ tầng.

BigQuery là serverless hoàn toàn — không cần provision compute node hay lo cấu trúc lưu trữ. Sandbox tier miễn phí 10GB lưu trữ và 1TB truy vấn/tháng, không cần thẻ tín dụng quốc tế. Batch load từ Delta Lake lên BigQuery qua Airflow cũng miễn phí. Tích hợp với dbt và Looker Studio mượt, nhờ kiến trúc tách compute khỏi storage (Colossus + Dremel).

Điểm yếu là không hợp cho khối lượng OLTP (ghi từng dòng liên tục), và bảng trong Sandbox tự xóa sau 60 ngày nếu không gia hạn.

PostgreSQL local miễn phí và chạy offline được, nhưng bản chất là database row-oriented cho xử lý giao dịch, không phản ánh đúng kiến trúc warehouse hiện đại. Snowflake mạnh nhưng trial chỉ 30 ngày và cần quản lý credit sát sao.

---

## 6. Transformation: dbt Core (thay vì SQL thuần trong Airflow hoặc stored procedures)

Mục tiêu là áp dụng tư duy kỹ thuật phần mềm vào việc biến đổi dữ liệu: modular hóa, version control, testing, documentation.

dbt tách bạch rõ tầng nguồn (`stg_orders`) khỏi tầng báo cáo (`fct_orders`, `dim_restaurants`), và chạy test toàn vẹn dữ liệu (`unique`, `not_null`) tự động sau mỗi lần build — chặn dữ liệu bẩn lọt xuống dashboard trước khi nó gây hại. Jinja/macro giúp tránh lặp lại SQL phức tạp nhiều lần, và dbt tự sinh tài liệu, vẽ luôn sơ đồ quan hệ giữa các bảng.

Giới hạn là dbt chỉ lo phần Transform (chữ T trong ELT) — Extract và Load vẫn phải dựa vào Airflow. Ngoài ra cũng đòi hỏi biết Jinja và cách quản lý profile kết nối.

Viết SQL thuần trong BigQueryOperator của Airflow thì dễ bắt đầu, nhưng khi số bảng tăng lên sẽ thành ác mộng quản lý lineage, và không có sẵn khung testing tự động.

---

## 7. Visualization: Google Looker Studio (thay vì Metabase chạy Docker)

Cần dashboard trực quan, hỗ trợ bản đồ nhiệt theo tọa độ, và chia sẻ được công khai qua link (ví dụ gắn vào GitHub).

Looker Studio kết nối BigQuery chỉ bằng một cú click, không cần mở port hay NAT IP từ máy cá nhân. Nó parse tọa độ kinh/vĩ độ thành bong bóng trên bản đồ TP.HCM khá tốt, và chia sẻ public dễ dàng — tiện để gắn link vào CV cho nhà tuyển dụng bấm vào xem trực tiếp.

Điểm trừ là khả năng quản lý dashboard dưới dạng code còn hạn chế so với Superset hay Evidence, và tùy biến CSS sâu thì gần như không có.

Metabase chạy Docker thì visualize nội bộ nhanh và đẹp, nhưng chạy local thì người ngoài không truy cập được qua internet nếu không setup tunnel — không phù hợp khi mục đích là cho nhà tuyển dụng xem.