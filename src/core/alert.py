import json
import urllib.request
from src.config.settings import ALERT_WEBHOOK_URL
from src.core.logger import get_logger

logger = get_logger(__name__)


def send_alert_webhook(message):
    if not ALERT_WEBHOOK_URL:
        return
    try:
        payload = json.dumps({"content": message, "text": message}).encode("utf-8")
        req = urllib.request.Request(
            ALERT_WEBHOOK_URL,
            data=payload,
            headers={"Content-Type": "application/json"},
            method="POST",
        )
        urllib.request.urlopen(req, timeout=5)
        logger.info("[ALERT WEBHOOK] Đã gửi thông báo cảnh báo qua Webhook thành công.")
    except Exception as e:
        logger.warning(f"[ALERT WEBHOOK] Không thể gửi thông báo qua Webhook: {e}")

