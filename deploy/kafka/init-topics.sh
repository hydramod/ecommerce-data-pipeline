#!/bin/bash
set -e

echo "Waiting for Kafka to be ready..."
sleep 30

# Create topics directly (they will fail gracefully if they already exist)
/opt/bitnami/kafka/bin/kafka-topics.sh --bootstrap-server ${KAFKA_BOOTSTRAP} --create --topic ${TOPIC_ORDER_EVENTS:-orders.events} --partitions 6 --replication-factor 1 || echo "Topic orders.events may already exist"
/opt/bitnami/kafka/bin/kafka-topics.sh --bootstrap-server ${KAFKA_BOOTSTRAP} --create --topic ${TOPIC_PAYMENT_EVENTS:-payments.events} --partitions 6 --replication-factor 1 || echo "Topic payments.events may already exist"
/opt/bitnami/kafka/bin/kafka-topics.sh --bootstrap-server ${KAFKA_BOOTSTRAP} --create --topic ${TOPIC_SHIPPING_EVENTS:-shipping.events} --partitions 6 --replication-factor 1 || echo "Topic shipping.events may already exist"

echo "Topic creation completed!"