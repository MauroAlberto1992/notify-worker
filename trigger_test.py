from celery_app import send_notification,send_notification_flaky

# Fires a handful of fake tasks manually, to confirm they reach the worker
# and get processed. Run this while the worker (celery_app.py) is running
# in another terminal.

fake_patients = [
    ("patient_1", "consultation_101"),
    ("patient_2", "consultation_102"),
    ("patient_3", "consultation_103"),
    ("patient_broken", "consultation_104"),
]

for patient_id, consultation_id in fake_patients:
    result = send_notification_flaky.delay(patient_id, consultation_id)
    print(f"Queued task {result.id} for patient_id={patient_id}")