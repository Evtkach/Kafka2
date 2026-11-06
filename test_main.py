import asyncio
import json
import pytest
from httpx import AsyncClient, ASGITransport
import main  # Импортируем ваше приложение

# Настраиваем pytest для работы с асинхронностью
pytestmark = pytest.mark.asyncio


@pytest.fixture(autouse=True)
def reset_global_producer():
    """Фикстура сбрасывает глобальный продюсер перед каждым тестом."""
    main.producer = None
    yield
    main.producer = None


@pytest.fixture
def mock_kafka(mocker):
    """Фикстура для изолирования aiokafka во всех тестах."""
    # Мокаем класс продюсера и его методы
    mock_prod_cls = mocker.patch("main.AIOKafkaProducer", autospec=True)
    mock_prod_instance = mock_prod_cls.return_value
    mock_prod_instance.start = mocker.AsyncMock()
    mock_prod_instance.stop = mocker.AsyncMock()

    # Имитируем возврат метаданных при отправке
    mock_meta = mocker.MagicMock()
    mock_meta.topic = "my_topic"
    mock_meta.partition = 0
    mock_meta.offset = 42
    mock_prod_instance.send_and_wait = mocker.AsyncMock(return_value=mock_meta)

    # Мокаем класс консьюмера
    mock_cons_cls = mocker.patch("main.AIOKafkaConsumer", autospec=True)
    mock_cons_instance = mock_cons_cls.return_value
    mock_cons_instance.start = mocker.AsyncMock()
    mock_cons_instance.stop = mocker.AsyncMock()

    # Бесконечный асинхронный генератор для консьюмеров
    async def async_iterable():
        while True:
            await asyncio.sleep(0.1)
            yield mocker.MagicMock()

    mock_cons_instance.__aiter__.side_effect = async_iterable

    return {
        "producer": mock_prod_instance,
        "consumer": mock_cons_instance
    }


# 1. Тест эндпоинта /health (без кафки)
async def test_health_check():
    async with AsyncClient(transport=ASGITransport(app=main.app), base_url="http://test") as ac:
        response = await ac.get("/health")
    assert response.status_code == 200
    assert response.json() == {"status": "healthy"}


# 2. Тест успешного прохождения Lifespan
async def test_lifespan_flow(mock_kafka):
    # Явно вызываем lifespan вашего приложения через асинхронный контекст
    async with main.lifespan(main.app):
        # Даем микросекунду фоновым задачам запуститься
        await asyncio.sleep(0.01)

        # Проверяем, что всё успешно стартануло
        mock_kafka["producer"].start.assert_called_once()
        assert main.producer is not None
        assert mock_kafka["consumer"].start.call_count == 3

    # При выходе из блока async with гарантированно сработает блок очистки (shutdown)
    mock_kafka["producer"].stop.assert_called_once()
    mock_kafka["consumer"].stop.call_count == 3


# 3. Тест успешной отправки сообщения через /send
async def test_send_message_success(mock_kafka):
    # Оборачиваем тест в lifespan, чтобы инициализировался продюсер
    async with main.lifespan(main.app):
        async with AsyncClient(transport=ASGITransport(app=main.app), base_url="http://test") as ac:
            payload = {"key": "user_1", "value": "hello_world"}
            response = await ac.post("/send", json=payload)

    assert response.status_code == 200
    res_data = response.json()
    assert res_data["status"] == "success"
    assert res_data["offset"] == 42

    expected_value = json.dumps("hello_world").encode("utf-8")
    expected_key = b"user_1"
    mock_kafka["producer"].send_and_wait.assert_called_once_with(
        "my_topic", value=expected_value, key=expected_key
    )


# 4. Тест ошибки: отправка, когда Продюсер НЕ инициализирован
async def test_send_message_no_producer():
    # Намеренно НЕ вызываем main.lifespan
    async with AsyncClient(transport=ASGITransport(app=main.app), base_url="http://test") as ac:
        payload = {"key": "test", "value": "data"}
        response = await ac.post("/send", json=payload)

    assert response.status_code == 500
    assert response.json()["detail"] == "Kafka Producer не инициализирован"


# 5. Тест ошибки: падение отправки внутри aiokafka (исключение брокера)
async def test_send_message_kafka_exception(mock_kafka):
    mock_kafka["producer"].send_and_wait.side_effect = Exception("Kafka connection lost")

    async with main.lifespan(main.app):
        async with AsyncClient(transport=ASGITransport(app=main.app), base_url="http://test") as ac:
            payload = {"key": "test", "value": "fail"}
            response = await ac.post("/send", json=payload)

    assert response.status_code == 500
    assert "Ошибка отправки: Kafka connection lost" in response.json()["detail"]
