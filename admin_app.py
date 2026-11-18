import os
import httpx
from fastapi import FastAPI, Request
from fastapi.responses import RedirectResponse
from sqladmin import Admin, ModelView, action
from models import Campaign, SessionLocal, Subscriber, engine
from worker import send_scheduled_digest_task

KAFKA_SERVICE_URL = os.getenv("KAFKA_SERVICE_URL", "http://127.0.0.1:8001")

app = FastAPI(title="Единый центр управления рассылками")
admin = Admin(app, engine, title="Mail Admin")


# --- Настройка раздела Подписчиков ---
class SubscriberAdmin(ModelView, model=Subscriber):
    name = "Подписчик"
    name_plural = "Подписчики"
    icon = "fa-solid fa-users"

    column_list = [Subscriber.id, Subscriber.email, Subscriber.name, Subscriber.is_active, Subscriber.created_at]
    column_searchable_list = [Subscriber.email, Subscriber.name]

    @action(
        name="trigger_celery_digest",
        label="Запустить дайджест Celery (RabbitMQ) сейчас",
        confirmation_message="Отправить задачу регулярного дайджеста в очередь RabbitMQ для всех активных подписчиков?",
        add_in_list=True,
    )
    async def trigger_celery_digest(self, request: Request):
        send_scheduled_digest_task.delay()
        return RedirectResponse(request.url_for("admin:list", identity=self.identity), status_code=302)


# --- Настройка раздела Рассылок (через Kafka) ---
class CampaignAdmin(ModelView, model=Campaign):
    name = "Рассылка (Kafka)"
    name_plural = "Рассылки (Kafka)"
    icon = "fa-solid fa-paper-plane"

    column_list = [Campaign.id, Campaign.title, Campaign.subject, Campaign.status, Campaign.created_at]
    column_searchable_list = [Campaign.title, Campaign.subject]

    @action(
        name="send_via_kafka",
        label="Отправить рассылку через Kafka",
        confirmation_message="Запустить массовую отправку этой рассылки через Kafka всем активным подписчикам?",
        add_in_detail=True,
        add_in_list=True,
    )
    async def send_via_kafka(self, request: Request):
        pks_raw = request.query_params.get("pks", "")
        pks = [pk for pk in pks_raw.split(",") if pk]
        if not pks:
            return RedirectResponse(request.url_for("admin:list", identity=self.identity), status_code=302)

        campaign_id = int(pks[0])

        with SessionLocal() as db:
            campaign = db.query(Campaign).filter(Campaign.id == campaign_id).first()
            active_subscribers = db.query(Subscriber).filter(Subscriber.is_active == True).all()
            emails = [sub.email for sub in active_subscribers]

            if campaign and emails:
                payload = {
                    "campaign_id": campaign.id,
                    "subject": campaign.subject,
                    "body": campaign.html_body,
                    "is_html": True,
                    "recipients": emails,
                }
                async with httpx.AsyncClient() as client:
                    response = await client.post(f"{KAFKA_SERVICE_URL}/send-campaign", json=payload, timeout=10.0)
                    response.raise_for_status()

                campaign.status = f"Отправлено в Kafka ({len(emails)} шт.)"
                db.commit()

        return RedirectResponse(request.url_for("admin:list", identity=self.identity), status_code=302)


admin.add_view(SubscriberAdmin)
admin.add_view(CampaignAdmin)


@app.get("/")
async def root():
    return RedirectResponse(url="/admin")
