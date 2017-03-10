import os
from celery import Celery
from utils import get_master_hostname


master_hostname = get_master_hostname()

app = Celery('example',
             broker='amqp://guest@{}//'.format(master_hostname),
             backend='amqp://guest@{}//'.format(master_hostname),
             include=['geoutils.tasks'])

app.conf.update(
    result_expires=300,
    worker_send_task_events=True,
    task_send_sent_event=True,
    task_time_limit=1800,
    task_soft_time_limit=1800,
    track_started=True,
    worker_prefetch_multiplier=1,
    worker_concurrency=1,
    task_acks_late=True,
    task_reject_on_worker_lost=True
)
