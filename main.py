import asyncio
import json
import logging
import os
from contextlib import asynccontextmanager
from email.message import EmailMessage
from typing import List, Optional

import aiosmtplib
from aiokafka import AIOKafkaConsumer, AIOKafkaProducer
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel, EmailStr

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger("KafkaEmailService")

# Конфигурация из переменных окружения
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "email_campaigns")
KAFKA_CONSUMER_GROUP = os.getenv("KAFKA_CONSUMER_GROUP", "email_sender_group")

# Адрес ВАШЕГО SMTP-сервера из Части 1
CUSTOM_SMTP_HOST = os.getenv("CUSTOM_SMTP_HOST", "localhost")
CUSTOM_SMTP_PORT = int(os.getenv("CUSTOM_SMTP_PORT", 1025))
SENDER_EMAIL = os.getenv("SENDER_EMAIL", "noreply@myservice.local")

producer: Optional[AIOKafkaProducer] = None


# Схема одного письма в Kafka
class EmailTaskSchema(BaseModel):
    recipient: EmailStr
    subject: str
    body: str
    is_html: bool = True
    campaign_id: Optional[int] = None


# Схема массовой рассылки (для кнопки в админке)
class CampaignRequestSchema(BaseModel):
    campaign_id: int
    subject: str
    body: str
    is_html: bool = True
    recipients: List[EmailStr]


# Функция отправки письма на ваш SMTP-сервер
async def send_via_custom_smtp(task: EmailTaskSchema):
    msg = EmailMessage()
    msg["From"] = SENDER_EMAIL
    msg["To"] = task.recipient
    msg["Subject"] = task.subject

    if task.is_html:
        msg.set_content(task.body, subtype="html")
    else:
        msg.set_content(task.body)

    await aiosmtplib.send(
        msg,
        hostname=CUSTOM_SMTP_HOST,
        port=CUSTOM_SMTP_PORT,
    )


# Воркер-консьюмер
async def run_consumer(consumer_id: int):
    consumer = AIOKafkaConsumer(
        KAFKA_TOPIC,
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        group_id=KAFKA_CONSUMER_GROUP,
        auto_offset_reset="earliest",
        enable_auto_commit=True,
    )
    await consumer.start()
    logger.info(f"Consumer-{consumer_id} запущен и слушает топик '{KAFKA_TOPIC}'")

    try:
        async for msg in consumer:
            try:
                raw_data = json.loads(msg.value.decode("utf-8"))
                email_task = EmailTaskSchema(**raw_data)

                await send_via_custom_smtp(email_task)
                logger.info(
                    f"Consumer-{consumer_id} отправил письмо на {email_task.recipient} "
                    f"(Campaign ID: {email_task.campaign_id}, Offset: {msg.offset})"
                )
            except Exception as e:
                logger.error(f"Consumer-{consumer_id} ошибка обработки сообщения (Offset {msg.offset}): {e}")
    except asyncio.CancelledError:
        logger.info(f"Consumer-{consumer_id} останавливается...")
    finally:
        await consumer.stop()
        logger.info(f"Consumer-{consumer_id} остановлен.")


@asynccontextmanager
async def lifespan(app: FastAPI):
    global producer
    producer = AIOKafkaProducer(bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS)
    await producer.start()
    logger.info("Kafka Producer успешно запущен.")

    consumer_tasks = [asyncio.create_task(run_consumer(i)) for i in range(1, 4)]

    yield

    logger.info("Остановка фоновых задач...")
    for task in consumer_tasks:
        task.cancel()
    await asyncio.gather(*consumer_tasks, return_exceptions=True)
    await producer.stop()
    logger.info("Сервис успешно завершил работу.")


app = FastAPI(title="Kafka Email Campaign Service", lifespan=lifespan)


# Ручка для одиночной отправки
@app.post("/send-one")
async def send_one_email(payload: EmailTaskSchema):
    if not producer:
        raise HTTPException(status_code=500, detail="Kafka Producer не инициализирован")

    message_bytes = payload.model_dump_json().encode("utf-8")
    key_bytes = payload.recipient.encode("utf-8")

    meta = await producer.send_and_wait(KAFKA_TOPIC, value=message_bytes, key=key_bytes)
    return {"status": "queued", "partition": meta.partition, "offset": meta.offset}


# Ручка для массовой рассылки по кнопке из Админки
@app.post("/send-campaign")
async def send_campaign(payload: CampaignRequestSchema):
    if not producer:
        raise HTTPException(status_code=500, detail="Kafka Producer не инициализирован")

    queued_count = 0
    for email in payload.recipients:
        task = EmailTaskSchema(
            recipient=email,
            subject=payload.subject,
            body=payload.body,
            is_html=payload.is_html,
            campaign_id=payload.campaign_id,
        )
        # Используем send() без ожидания каждого ответа для высокой скорости, затем flush()
        await producer.send(
            KAFKA_TOPIC,
            value=task.model_dump_json().encode("utf-8"),
            key=email.encode("utf-8"),
        )
        queued_count += 1

    await producer.flush()
    return {"status": "campaign_queued", "campaign_id": payload.campaign_id, "total_emails": queued_count}


@app.get("/health")
async def health_check():
    return {"status": "healthy"}
