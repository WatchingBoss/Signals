import asyncio
from rabbitmq import RabbitMQConsumer


async def main():
    consumer = RabbitMQConsumer()

    try:
        await consumer.connect()
        await consumer.start_consuming()

        # Блокируем выполнение, чтобы скрипт не завершился и продолжал слушать очередь
        await asyncio.Future()

    except KeyboardInterrupt:
        print("\n⏹ Работа прервана пользователем.")
    finally:
        await consumer.close()


if __name__ == "__main__":
    asyncio.run(main())