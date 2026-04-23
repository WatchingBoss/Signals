import aio_pika
import json

from message_announcer import MessageAnnouncer


class RabbitMQPublisher:
    def __init__(self, amqp_url="amqp://guest:guest@localhost/"):
        self.amqp_url = amqp_url
        self.connection = None
        self.channel = None

    async def connect(self):
        """Устанавливает устойчивое (robust) соединение."""
        # connect_robust автоматически переподключается при сбоях сети
        self.connection = await aio_pika.connect_robust(self.amqp_url)
        self.channel = await self.connection.channel()

        # Декларируем очередь (durable=True сохранит сообщения при рестарте брокера)
        await self.channel.declare_queue("trading_alerts", durable=True)
        print("✅ Подключено к RabbitMQ. Очередь: trading_alerts")

    async def publish_batch(self, alerts: list[dict]):
        """Отправляет массив словарей в виде JSON-сообщений."""
        if not alerts or not self.channel:
            return

        for alert in alerts:
            message = aio_pika.Message(
                body=json.dumps(alert).encode(),
                delivery_mode=aio_pika.DeliveryMode.PERSISTENT  # Сохраняем на диск
            )

            # Публикуем напрямую в очередь
            await self.channel.default_exchange.publish(
                message,
                routing_key="trading_alerts"
            )

        print(f"📤 Отправлено {len(alerts)} алертов в RabbitMQ.")

    async def close(self):
        if self.connection:
            await self.connection.close()


class RabbitMQConsumer:
    def __init__(self, message_announcer: MessageAnnouncer,
                 amqp_url="amqp://guest:guest@localhost/"):
        self.amqp_url = amqp_url
        self.announcer = message_announcer
        self.connection = None
        self.channel = None
        self.queue = None

    async def connect(self):
        """Устанавливает устойчивое (robust) соединение."""
        # connect_robust автоматически переподключается при сбоях
        self.connection = await aio_pika.connect_robust(self.amqp_url)
        self.channel = await self.connection.channel()

        # Настраиваем QoS. prefetch_count=10 означает, что консюмер возьмет
        # не более 10 сообщений за раз, пока не подтвердит их обработку.
        # Это предотвращает перегрузку памяти консюмера.
        await self.channel.set_qos(prefetch_count=10)

        # Декларируем ту же очередь с durable=True.
        # Если консюмер запустится раньше паблишера, очередь всё равно будет создана.
        self.queue = await self.channel.declare_queue("trading_alerts", durable=True)
        print("✅ Консюмер подключен к RabbitMQ. Очередь: trading_alerts")

    async def process_message(self, message: aio_pika.IncomingMessage):
        """Callback для обработки каждого полученного сообщения."""
        # Контекстный менеджер process() автоматически отправляет ACK (подтверждение),
        # если блок кода выполнился без ошибок, и NACK, если выпало исключение.
        async with message.process():
            try:
                # Декодируем байты в строку, а затем парсим JSON
                alert = json.loads(message.body.decode())

                # Здесь должна быть ваша бизнес-логика
                print(f"📥 Получен алерт: {alert}")

                # Передает алерт в диспетчер
                await self.announcer.broadcast(alert)

            except json.JSONDecodeError:
                print("❌ Ошибка: Не удалось декодировать JSON.")
            except Exception as e:
                print(f"❌ Непредвиденная ошибка при обработке сообщения: {e}")

    async def start_consuming(self):
        """Запускает процесс прослушивания очереди."""
        if not self.channel:
            await self.connect()

        print("⏳ Ожидание сообщений. Для выхода нажмите CTRL+C")
        # Начинаем потреблять сообщения, передавая callback-функцию
        await self.queue.consume(self.process_message)

    async def close(self):
        """Закрывает соединение с брокером."""
        if self.connection:
            await self.connection.close()
            print("🔌 Соединение консюмера закрыто.")