import asyncio
import json
import logging
from contextlib import asynccontextmanager
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from aiokafka import AIOKafkaProducer, AIOKafkaConsumer

# Настройки логирования
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Константы конфигурации
KAFKA_BOOTSTRAP_SERVERS = "localhost:9092"
KAFKA_TOPIC = "my_topic"
KAFKA_CONSUMER_GROUP = "my_consumer_group"

# Глобальный продьюсер
producer: AIOKafkaProducer = None


# Схема данных для API
class MessageSchema(BaseModel):
    key: str
    value: str


# Функция для работы отдельного консьюмера
async def run_consumer(consumer_id: int):
    consumer = AIOKafkaConsumer(
        KAFKA_TOPIC,
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        group_id=KAFKA_CONSUMER_GROUP,
        auto_offset_reset="earliest",
        enable_auto_commit=True
    )
    await consumer.start()
    logger.info(f" Consumer-{consumer_id} успешно запущен и подписан на топик '{KAFKA_TOPIC}'")

    try:
        async for msg in consumer:
            logger.info(
                f" Consumer-{consumer_id} получил сообщение: "
                f"Partition={msg.partition}, Offset={msg.offset}, "
                f"Key={msg.key.decode() if msg.key else None}, Value={msg.value.decode()}"
            )
    except asyncio.CancelledError:
        logger.info(f" Consumer-{consumer_id} останавливается...")
    finally:
        await consumer.stop()
        logger.info(f" Consumer-{consumer_id} остановлен.")


# Управление жизненным циклом FastAPI (Lifespan)
@asynccontextmanager
async def lifespan(app: FastAPI):
    global producer

    # 1. Инициализация и запуск Продьюсера
    producer = AIOKafkaProducer(bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS)
    await producer.start()
    logger.info(" Producer успешно запущен.")

    # 2. Запуск 3-х Консьюмеров в фоне в одной Consumer Group
    consumer_tasks = []
    for i in range(1, 4):
        task = asyncio.create_task(run_consumer(consumer_id=i))
        consumer_tasks.append(task)

    yield  # Здесь приложение работает и принимает HTTP-запросы

    # 3. Очистка при выключении приложения
    logger.info("Остановка фоновых задач и ресурсов...")

    # Отменяем задачи консьюмеров
    for task in consumer_tasks:
        task.cancel()
    await asyncio.gather(*consumer_tasks, return_exceptions=True)

    # Останавливаем продьюсер
    await producer.stop()
    logger.info(" Приложение успешно завершило работу.")


# Инициализация FastAPI с lifespan-менеджером
app = FastAPI(title="FastAPI + Kafka Service", lifespan=lifespan)


# Эндпоинт отправки сообщения (Продьюсер)
@app.post("/send")
async def send_message(payload: MessageSchema):
    if not producer:
        raise HTTPException(status_code=500, detail="Kafka Producer не инициализирован")

    try:
        # Сериализуем данные в JSON байты
        message_bytes = json.dumps(payload.value).encode("utf-8")
        key_bytes = payload.key.encode("utf-8")

        # Отправляем сообщение в Kafka
        meta = await producer.send_and_wait(KAFKA_TOPIC, value=message_bytes, key=key_bytes)

        return {
            "status": "success",
            "topic": meta.topic,
            "partition": meta.partition,
            "offset": meta.offset
        }
    except Exception as e:
        logger.error(f"Ошибка при отправке сообщения: {e}")
        raise HTTPException(status_code=500, detail=f"Ошибка отправки: {str(e)}")


# Эндпоинт проверки работоспособности
@app.get("/health")
async def health_check():
    return {"status": "healthy"}
