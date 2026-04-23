from fastapi import FastAPI, Request
from fastapi.responses import StreamingResponse
from fastapi.middleware.cors import CORSMiddleware
from contextlib import asynccontextmanager

import asyncio
import json

from message_announcer import MessageAnnouncer
from rabbitmq import RabbitMQConsumer


# Инициализируем наши классы
announcer = MessageAnnouncer()
consumer = RabbitMQConsumer(announcer)


@asynccontextmanager
async def lifespan(app: FastAPI):
    # Стартуем при запуске FastAPI
    await consumer.connect()
    # Запускаем чтение RabbitMQ как фоновую задачу (task)
    consumer_task = asyncio.create_task(consumer.start_consuming())

    yield  # Здесь работает сервер FastAPI

    # Останавливаем при выключении сервера
    consumer_task.cancel()
    await consumer.close()


app = FastAPI(lifespan=lifespan)

# Разрешаем CORS, если фронтенд будет на другом порту
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.get("/stream")
async def sse_endpoint(request: Request):
    """Ендпоинт для подключения по SSE."""

    async def event_generator():
        # Создаем персональную очередь для этого HTTP-запроса
        q = announcer.listen()
        try:
            while True:
                # Ждем появление нового сообщения (блокирует только эту корутину)
                msg = await q.get()

                # Формат SSE требует префикса 'data:' и двух переносов строки
                yield f"data: {json.dumps(msg, ensure_ascii=False)}\n\n"

        except asyncio.CancelledError:
            # Срабатывает, когда клиент закрывает вкладку браузера или обрывает связь
            print("🚫 Клиент отключился")
        finally:
            # Обязательно удаляем очередь, чтобы не было утечек памяти
            announcer.remove(q)

    # Возвращаем стрим с правильным MIME-типом для SSE
    return StreamingResponse(event_generator(), media_type="text/event-stream")