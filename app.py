from fastapi import FastAPI
from pydantic import EmailStr
from worker import process_user_report, send_scheduled_digest_task

app = FastAPI(title="FastAPI + Celery Service")


@app.post("/generate-report/{user_id}")
async def trigger_background_report(user_id: int, user_email: EmailStr = "client@example.com"):
    task = process_user_report.delay(user_id, user_email)
    return {
        "status": "Task dispatched to worker",
        "task_id": task.id
    }


@app.post("/trigger-digest-now")
async def trigger_digest_now():
    """Ручной запуск задачи дайджеста через RabbitMQ."""
    task = send_scheduled_digest_task.delay()
    return {
        "status": "Digest task queued in RabbitMQ",
        "task_id": task.id
    }
