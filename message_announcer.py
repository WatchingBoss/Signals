import asyncio
import json

class MessageAnnouncer:
    def __init__(self):
        # Список очередей для каждого подключенного SSE-клиента
        self.listeners: list[asyncio.Queue] = []

    def listen(self) -> asyncio.Queue:
        """Создает новую очередь для нового клиента и добавляет её в список."""
        q = asyncio.Queue()
        self.listeners.append(q)
        return q

    def remove(self, q: asyncio.Queue):
        """Удаляет очередь, когда клиент отключается."""
        if q in self.listeners:
            self.listeners.remove(q)

    async def broadcast(self, message: dict):
        """Отправляет сообщение во все активные очереди клиентов."""
        for q in self.listeners:
            await q.put(message)