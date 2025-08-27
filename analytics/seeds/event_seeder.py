#!/usr/bin/env python3
"""Simple e-commerce event seeder for Kafka.
Tries `confluent_kafka` first; falls back to `kafka-python`.
Usage:
  python event_seeder.py --bootstrap kafka:9092 --rate 2.0
"""
import json, uuid, random, time, argparse, sys, datetime

def now_iso():
    return datetime.datetime.utcnow().isoformat() + "Z"

class ProducerIface:
    def __init__(self, bootstrap):
        self.bootstrap = bootstrap
        self.impl = None
        self.mode = None
        try:
            from confluent_kafka import Producer
            self.impl = Producer({'bootstrap.servers': bootstrap})
            self.mode = 'confluent_kafka'
        except Exception as e:
            try:
                from kafka import KafkaProducer
                self.impl = KafkaProducer(bootstrap_servers=[bootstrap], value_serializer=lambda v: json.dumps(v).encode('utf-8'), key_serializer=lambda k: k.encode('utf-8'))
                self.mode = 'kafka_python'
            except Exception as e2:
                print("Neither confluent_kafka nor kafka-python available. Please `pip install confluent-kafka` or `pip install kafka-python`.", file=sys.stderr)
                sys.exit(1)

    def produce(self, topic, key, value):
        if self.mode == 'confluent_kafka':
            self.impl.produce(topic, key=key.encode('utf-8'), value=json.dumps(value))
            self.impl.poll(0)
        else:
            self.impl.send(topic, key=key, value=value)

    def flush(self):
        try:
            self.impl.flush()
        except:
            pass

SKUS = ['A123','B234','C345','D456','E567']

def make_order():
    order_id = str(uuid.uuid4())
    items = [{
        'sku': random.choice(SKUS),
        'qty': random.randint(1,3),
        'price': round(random.uniform(5,200),2)
    } for _ in range(random.randint(1,3))]
    total = round(sum(i['qty']*i['price'] for i in items),2)
    evt = {
        'event_id': str(uuid.uuid4()),
        'event_type': 'order_created',
        'order_id': order_id,
        'user_id': str(uuid.uuid4()),
        'items': items,
        'currency': 'EUR',
        'total_amount': total,
        'status': 'created',
        'event_time': now_iso(),
        'ingest_ts': now_iso()
    }
    return order_id, evt, total

def make_payment(order_id, amount):
    evt = {
        'event_id': str(uuid.uuid4()),
        'payment_id': str(uuid.uuid4()),
        'order_id': order_id,
        'amount': amount,
        'currency': 'EUR',
        'method': random.choice(['card', 'paypal', 'apple_pay', 'google_pay']),
        'status': random.choices(['captured','failed'], weights=[0.9,0.1])[0],
        'event_time': now_iso(),
        'ingest_ts': now_iso()
    }
    return evt

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('--bootstrap', default='kafka:9092')
    ap.add_argument('--rate', type=float, default=1.0, help='events per second (orders)')
    ap.add_argument('--payments', type=float, default=0.85, help='probability an order gets a payment')
    args = ap.parse_args()

    prod = ProducerIface(args.bootstrap)
    print(f"Producing to {args.bootstrap} (mode={prod.mode})... Press Ctrl+C to stop.")

    try:
        while True:
            oid, order_evt, total = make_order()
            prod.produce('orders.v1', key=oid, value=order_evt)
            if random.random() < args.payments:
                pay_evt = make_payment(oid, total)
                prod.produce('payments.v1', key=oid, value=pay_evt)
            prod.flush()
            time.sleep(max(0.01, 1.0/args.rate))
    except KeyboardInterrupt:
        print("Stopping...")
        prod.flush()

if __name__ == '__main__':
    main()