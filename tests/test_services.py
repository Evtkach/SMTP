import socket
from email.message import EmailMessage
from unittest.mock import AsyncMock, MagicMock, patch

import aiosmtplib
import pytest
from aiosmtpd.controller import Controller
from fastapi.testclient import TestClient

import admin_app
import main
import worker
from models import Campaign, Subscriber
from smtp_server import CustomSMTPHandler


def get_free_port():
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


# --- 1. Интеграционный тест кастомного SMTP-сервера ---
@pytest.mark.asyncio
async def test_custom_smtp_server_receives_email():
    port = get_free_port()
    handler = CustomSMTPHandler()
    controller = Controller(handler, hostname="127.0.0.1", port=port)
    controller.start()

    try:
        msg = EmailMessage()
        msg["From"] = "test@local.dev"
        msg["To"] = "client@example.com"
        msg["Subject"] = "Проверка SMTP"
        msg.set_content("Привет, это реальный SMTP тест!")

        response = await aiosmtplib.send(msg, hostname="127.0.0.1", port=port)
        assert "Message accepted for delivery" in response[1]
    finally:
        controller.stop()


# --- 2. Тесты Kafka-сервиса (FastAPI Producer) ---
def test_kafka_service_send_campaign():
    mock_producer = AsyncMock()
    mock_meta = MagicMock(topic="email_campaigns", partition=0, offset=42)
    mock_producer.send_and_wait.return_value = mock_meta

    with patch.object(main, "producer", mock_producer):
        client = TestClient(main.app)

        # Проверяем healthcheck
        res_health = client.get("/health")
        assert res_health.status_code == 200
        assert res_health.json() == {"status": "healthy"}

        # Проверяем массовую рассылку
        payload = {
            "campaign_id": 10,
            "subject": "Акция",
            "body": "<b>Скидка 50%</b>",
            "is_html": True,
            "recipients": ["a@test.ru", "b@test.ru"],
        }
        res = client.post("/send-campaign", json=payload)
        assert res.status_code == 200
        data = res.json()
        assert data["status"] == "campaign_queued"
        assert data["total_emails"] == 2
        assert mock_producer.send.call_count == 2
        mock_producer.flush.assert_awaited_once()


# --- 3. Тест Celery-воркера и фильтрации подписчиков из БД ---
def test_celery_digest_sends_only_to_active_subscribers(db_session, monkeypatch):
    # Добавляем 2 активных и 1 неактивного подписчика
    db_session.add_all([
        Subscriber(email="active1@test.ru", name="Active 1", is_active=True),
        Subscriber(email="active2@test.ru", name="Active 2", is_active=True),
        Subscriber(email="inactive@test.ru", name="Inactive", is_active=False),
    ])
    db_session.commit()

    monkeypatch.setattr(worker, "SessionLocal", lambda: db_session)

    with patch("worker.send_email_via_custom_smtp") as mock_smtp:
        result = worker.send_scheduled_digest_task()

        assert "на 2 адресов" in result
        mock_smtp.assert_called_once()
        sent_recipients = mock_smtp.call_args[0][0]
        assert set(sent_recipients) == {"active1@test.ru", "active2@test.ru"}
        assert "inactive@test.ru" not in sent_recipients


# --- 4. Тесты Админки (FastAPI + SQLAdmin) ---
def test_admin_panel_pages_load(db_session, monkeypatch):
    monkeypatch.setattr(admin_app, "SessionLocal", lambda: db_session)
    client = TestClient(admin_app.app)

    # Главная страница админки
    res_index = client.get("/admin/", follow_redirects=True)
    assert res_index.status_code == 200

    # Список подписчиков
    res_subs = client.get("/admin/subscriber/list")
    assert res_subs.status_code == 200

    # Список рассылок
    res_camps = client.get("/admin/campaign/list")
    assert res_camps.status_code == 200
