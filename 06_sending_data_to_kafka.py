from confluent_kafka import Producer
import time

# === Параметры подключения к Kafka ===
kafka_brokers = 'kafka-01.msk.local:9092,' \
                'kafka-02.msk.local:9092,' \
                'kafka-03.msk.local:9092'
topic = 'dp.event-streaming.stats'


def delivery_report(err, msg):
    """Вызывается при подтверждении доставки сообщения."""
    if err is not None:
        print(f'❌ Ошибка доставки сообщения: {err}')
    else:
        print(f'✅ Сообщение доставлено. Топик: {msg.topic()}, Раздел: {msg.partition()}, Смещение: {msg.offset()}')


def produce_messages(producer, topic, num_messages=10):
    """Производит сообщения и отправляет их в Kafka."""
    for i in range(num_messages):
        key = f'key-{i}'
        value = f'Это тестовое сообщение №{i + 1}'
        try:
            producer.produce(
                topic=topic,
                key=key,
                value=value,
                callback=delivery_report
            )
            print(f'📤 Отправлено сообщение: {value}')
        except Exception as e:
            print(f'❌ Ошибка отправки сообщения: {e}')

        # Вставляем небольшую задержку для наглядности работы
        time.sleep(1)

    # Ожидание завершения всех поставок
    producer.flush()


def main():
    """Основная функция для инициализации и отправки сообщений."""
    # Конфигурация Kafka Producer
    producer_conf = {
        'bootstrap.servers': kafka_brokers,
        'client.id': 'python-producer'
    }

    # Инициализация Kafka Producer
    producer = Producer(producer_conf)

    print(f"📡 Подключение к Kafka брокерам: {kafka_brokers}")
    print(f"🎯 Отправка сообщений в топик: {topic}")

    # Производим и отправляем сообщения
    produce_messages(producer, topic, num_messages=5)  # Укажите количество сообщений для отправки


if __name__ == "__main__":
    main()
