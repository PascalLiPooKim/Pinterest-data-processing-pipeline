sudo docker run -d \
    --rm -p 9090:9090 \
    --name prometheus \
    -v /home/aicore/AiCore/Pinterest-data-processing-pipeline/prometheus.yml:/etc/prometheus/prometheus.yml \
    prom/prometheus --config.file=/etc/prometheus/prometheus.yml \
    --web.enable-lifecycle

# curl -X POST http://localhost:9090/-/reload

# /home/aicore/node_exporter-1.1.2.linux-amd64/node_exporter --web.listen-address 172.17.0.1:9100


sudo docker run \
    -d --rm \
    --net=host \
    -e DATA_SOURCE_NAME="postgresql://postgres:aicore@localhost:5432/postgres?sslmode=disable" \
    quay.io/prometheuscommunity/postgres-exporter