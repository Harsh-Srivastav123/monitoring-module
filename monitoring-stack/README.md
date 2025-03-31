# Monitoring Stack

This is a complete monitoring stack using:
- Prometheus
- Cloudwatch Exporter
- Grafana
- ELK Stack (Elasticsearch, Logstash, Kibana)

## Setup

1. Clone this repository
2. Configure the AWS credentials for Cloudwatch Exporter
3. Run `docker-compose up -d`

## Access Points

- Grafana: http://localhost:3000 (admin/admin)
- Prometheus: http://localhost:9090
- Kibana: http://localhost:5601
- Elasticsearch: http://localhost:9200
- Cloudwatch Exporter: http://localhost:9106

## Configuration

- Update `cloudwatch-exporter/config.yml` to modify AWS CloudWatch metrics collection
- Modify Grafana dashboards in `grafana/provisioning/dashboards/`
- Adjust Logstash pipeline in `logstash/pipeline/logstash.conf` 