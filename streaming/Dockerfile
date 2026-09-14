FROM apache/spark:3.5.0-python3

USER root

# Cài đặt delta-spark
RUN pip install --no-cache-dir delta-spark==3.1.0

# Tạo thư mục ivy cache và cấp quyền ghi cho mọi user
RUN mkdir -p /tmp/.ivy2/cache /tmp/.ivy2/jars && chmod -R 777 /tmp/.ivy2

USER spark