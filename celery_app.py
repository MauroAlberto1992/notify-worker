import json
from datetime import datetime, timezone
 
import redis
from celery import Celery

REDIS_URL = "redis://localhost:6379/0"

# Redis running locally via docker-compose, acting as the broker (the queue)
app = Celery(
    "espion_notify_worker",
    broker="redis://localhost:6379/0",
    backend="redis://localhost:6379/0",  # stores task results/state, optional but useful for debugging
)

# Plain redis client, used directly (not through Celery) just to write
# the failure record — this is Option A: a simple log of failures,
# meant for observability (e.g. a Grafana dashboard reading this list),
# not for automatic action.
redis_client = redis.Redis.from_url(REDIS_URL)


@app.task(name="send_notification")
def send_notification(patient_id: str, consultation_id: str):
    """
    Placeholder task — for now it just logs.
    Later this will generate + 'send' an actual notification.
    """
    print(f"[send_notification] patient_id={patient_id} consultation_id={consultation_id}")
    return {"patient_id": patient_id, "consultation_id": consultation_id, "status": "processed"}


@app.task(
    name="send_notification_flaky",
    bind=True,  # bind=True gives access to `self`, needed to call self.retry()
    autoretry_for=(Exception,),  # which exceptions trigger an automatic retry
    retry_backoff=True,          # exponential backoff: 1s, 2s, 4s, 8s...
    retry_backoff_max=60,        # cap the backoff at 60s
    retry_jitter=True,           # adds randomness to avoid retry "thundering herd"
    max_retries=3,               # give up after 3 attempts
)
def send_notification_flaky(self, patient_id: str, consultation_id: str):
    """
    Deliberately fails for one specific fake patient, to observe
    Celery's retry + exponential backoff behavior in the logs.
    """
    print(
        f"[send_notification_flaky] attempt={self.request.retries + 1} "
        f"patient_id={patient_id} consultation_id={consultation_id}"
    )
 
    if patient_id == "patient_broken":
        raise Exception(f"Simulated failure for {patient_id}")
 
    return {"patient_id": patient_id, "consultation_id": consultation_id, "status": "processed"}

app.task(bind=True)
def on_failure_hook_example(self):
    """Not used directly — see the on_failure method wired below instead."""
    pass
 
 
def _record_failure(task_id, patient_id, consultation_id, error_message):
    """
    Writes a structured failure record into a Redis list called
    'failed_notifications'. This is the 'DLQ as a log' — a place to
    look at (or point a dashboard at) after the fact.
    """
    record = {
        "task_id": task_id,
        "patient_id": patient_id,
        "consultation_id": consultation_id,
        "error": error_message,
        "failed_at": datetime.now(timezone.utc).isoformat(),
    }
    redis_client.lpush("failed_notifications", json.dumps(record))
    print(f"[DLQ] Recorded permanent failure: {record}")
 
 
# Celery lets you attach an on_failure handler per-task via a subclass,
# but the simplest approach for a single task is to override it directly
# on the task instance after definition:
def _send_notification_flaky_on_failure(exc, task_id, args, kwargs, einfo):
    patient_id = kwargs.get("patient_id") or (args[0] if args else "unknown")
    consultation_id = kwargs.get("consultation_id") or (args[1] if len(args) > 1 else "unknown")

    # Option A: log the failure for observability (e.g. a Grafana dashboard)
    _record_failure(task_id, patient_id, consultation_id, str(exc))
    # Option B: dispatch to the dedicated failed_queue, for automatic action
    handle_failed_notification.delay(task_id, patient_id, consultation_id, str(exc))

 
 
send_notification_flaky.on_failure = _send_notification_flaky_on_failure


# ---------------------------------------------------------------------------
# Option B: a dedicated queue, consumed by a task that takes automatic
# action (here: simulating "send an alert email") when a notification
# has permanently failed. This is triggered from the on_failure hook
# above, alongside the Option A log record.
# ---------------------------------------------------------------------------
 
@app.task(name="handle_failed_notification", queue="failed_queue")
def handle_failed_notification(task_id: str, patient_id: str, consultation_id: str, error_message: str):
    """
    Consumes from the dedicated 'failed_queue'. In a real system this
    would send an actual alert email (e.g. via Mailtrap/Resend) to an
    ops/admin address. For now it just prints a clear alert.
    """
    print(
        f"🚨 ALERT: notification permanently failed — "
        f"task_id={task_id} patient_id={patient_id} "
        f"consultation_id={consultation_id} error={error_message}"
    )
    # Real version would do something like:
    # send_alert_email(to="ops@espion.app", subject="Notification failed", body=...)
    return {"alerted": True, "task_id": task_id}