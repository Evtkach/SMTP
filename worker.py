import os
import smtplib
import time
from email.message import EmailMessage
from celery import Celery
from celery.schedules import crontab
from models import SessionLocal, Subscriber

# Читаем переменные окружения
RABBITMQ_URL = os.getenv("RABBITMQ_URL", "amqp://guest:guest@localhost:5672//")
POSTGRES_URL = os.getenv("DATABASE_URL", "db+postgresql://user:password@localhost:5432/dbname")

# Настройки вашего кастомного SMTP-сервера
CUSTOM_SMTP_HOST = os.getenv("CUSTOM_SMTP_HOST", "localhost")
CUSTOM_SMTP_PORT = int(os.getenv("CUSTOM_SMTP_PORT", 1025))
SENDER_EMAIL = os.getenv("SENDER_EMAIL", "scheduler@myservice.local")

# Инициализируем Celery
celery_app = Celery(
    "tasks",
    broker=RABBITMQ_URL,
    backend=POSTGRES_URL
)

celery_app.conf.timezone = "Europe/Moscow"


# --- Вспомогательная функция отправки через ваш SMTP-сервер ---

def send_email_via_custom_smtp(recipients: list[str], subject: str, html_body: str):
    with smtplib.SMTP(CUSTOM_SMTP_HOST, CUSTOM_SMTP_PORT) as server:
        for recipient in recipients:
            msg = EmailMessage()
            msg["From"] = SENDER_EMAIL
            msg["To"] = recipient
            msg["Subject"] = subject
            msg.set_content(html_body, subtype="html")
            server.send_message(msg)


# --- Описание задач ---

@celery_app.task(name="send_scheduled_digest_task")
def send_scheduled_digest_task(recipients: list[str] | None = None):
    """Периодическая рассылка писем активным подписчикам из PostgreSQL через кастомный SMTP."""
    if recipients:
        target_emails = recipients
    else:
        with SessionLocal() as db:
            active_subs = db.query(Subscriber).filter(Subscriber.is_active == True).all()
            target_emails = [sub.email for sub in active_subs]

    if not target_emails:
        print("Нет активных подписчиков в базе для отправки дайджеста.")
        return "Пропущено: нет активных подписчиков"

    subject = "Регулярный дайджест по расписанию (Celery Beat)"
    html_body = "<h2>Привет!</h2><p>Это автоматический дайджест из базы PostgreSQL через Celery Beat и ваш SMTP-сервер.</p>"

    print(f"Отправка дайджеста для {len(target_emails)} подписчиков через {CUSTOM_SMTP_HOST}:{CUSTOM_SMTP_PORT}...")
    send_email_via_custom_smtp(target_emails, subject, html_body)
    return f"Дайджест успешно отправлен на {len(target_emails)} адресов"


@celery_app.task(name="sync_database_task")
def sync_database_task():
    """Задача синхронизации базы данных."""
    print("Запуск периодической синхронизации базы данных...")
    return "Синхронизация успешно завершена"


@celery_app.task(name="process_user_report")
def process_user_report(user_id: int, user_email: str = "user@example.com"):
    """Генерация отчета с последующей отправкой уведомления на почту."""
    print(f"Началась генерация тяжелого отчета для пользователя {user_id}...")
    time.sleep(5)

    # Уведомляем пользователя о готовности отчета через ваш SMTP-сервер
    send_email_via_custom_smtp(
        recipients=[user_email],
        subject=f"Ваш отчет #{user_id} готов",
        html_body=f"<p>Отчет для пользователя <b>ID {user_id}</b> успешно сформирован.</p>"
    )
    return f"Отчет для {user_id} готов и отправлен на {user_email}"


# --- Настройка Celery Beat (Расписание) ---

celery_app.conf.beat_schedule = {
    # Тестовая рассылка каждые 30 секунд (чтобы сразу увидеть работу в логах SMTP)
    "send-digest-every-30-seconds": {
        "task": "send_scheduled_digest_task",
        "schedule": 30.0,
    },
    # Пример боевого расписания: каждый понедельник в 09:00 утра
    "send-weekly-digest-monday": {
        "task": "send_scheduled_digest_task",
        "schedule": crontab(hour=9, minute=0, day_of_week="monday"),
    },
    "run-sync-every-midnight": {
        "task": "sync_database_task",
        "schedule": crontab(hour=0, minute=0),
    },
}
