import asyncio
import logging
import os
from email import policy
from email.parser import BytesParser
import aiosmtplib
from aiosmtpd.controller import Controller

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger("CustomSMTPServer")

# Настройки самого SMTP-сервера
SMTP_HOST = os.getenv("SMTP_HOST", "0.0.0.0")
SMTP_PORT = int(os.getenv("SMTP_PORT", 1025))

# Настройки внешнего релея (например, локальный Mailpit или реальный Яндекс/Gmail)
RELAY_HOST = os.getenv("RELAY_HOST", "")  # Например: "mailpit" или "smtp.yandex.ru"
RELAY_PORT = int(os.getenv("RELAY_PORT", 1025))
RELAY_USER = os.getenv("RELAY_USER", "")
RELAY_PASS = os.getenv("RELAY_PASS", "")
RELAY_USE_TLS = os.getenv("RELAY_USE_TLS", "false").lower() == "true"


class CustomSMTPHandler:
    async def handle_DATA(self, server, session, envelope):
        peer = session.peer
        mail_from = envelope.mail_from
        rcpt_tos = envelope.rcpt_tos

        # Парсим сырые байты письма в объект EmailMessage
        msg = BytesParser(policy=policy.default).parsebytes(envelope.content)
        subject = msg.get("subject", "(Без темы)")

        logger.info(f"Получено письмо от {peer}: From={mail_from} -> To={rcpt_tos} | Subject='{subject}'")

        # Если настроен релей (Mailpit или внешний провайдер), пересылаем письмо дальше
        if RELAY_HOST:
            try:
                await aiosmtplib.send(
                    msg,
                    hostname=RELAY_HOST,
                    port=RELAY_PORT,
                    username=RELAY_USER or None,
                    password=RELAY_PASS or None,
                    use_tls=RELAY_USE_TLS,
                )
                logger.info(f"Письмо для {rcpt_tos} успешно передано на релей {RELAY_HOST}:{RELAY_PORT}")
            except Exception as e:
                logger.error(f"Ошибка при пересылке на релей: {e}")
                return f"451 Requested action aborted: relay error ({e})"
        else:
            # Режим автономного сервера: просто сохраняем/логируем отправку
            body = msg.get_body(preferencelist=("plain", "html"))
            content_preview = body.get_content()[:100] if body else ""
            logger.info(f"[Автономная отправка] Текст письма: {content_preview}...")

        return "250 Message accepted for delivery"


async def main():
    handler = CustomSMTPHandler()
    controller = Controller(handler, hostname=SMTP_HOST, port=SMTP_PORT)
    controller.start()
    logger.info(f"Собственный SMTP-сервер запущен на {SMTP_HOST}:{SMTP_PORT}")

    try:
        # Держим сервер запущенным бесконечно
        await asyncio.Event().wait()
    except KeyboardInterrupt:
        logger.info("Остановка SMTP-сервера...")
    finally:
        controller.stop()


if __name__ == "__main__":
    asyncio.run(main())
