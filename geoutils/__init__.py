import os
from celery import Celery
from utils import get_master_hostname


master_hostname = get_master_hostname()

app = Celery('example',
             broker='amqp://guest@{}//'.format(master_hostname),
             backend='amqp://guest@{}//'.format(master_hostname),
             include=['geoutils.tasks'])

app.conf.update(
    CELERY_TASK_RESULT_EXPIRES=300,
    CELERY_SEND_EVENTS=True,
    CELERY_SEND_TASK_SENT_EVENT=True,
    CELERY_TASK_SERIALIZER="json",
    CELERY_TASK_TIME_LIMIT=1800,
    CELERY_TASK_SOFT_TIME_LIMIT=1800,
    CELERY_TRACK_STARTED=True,
    CELERYD_PREFETCH_MULTIPLIER=1
)
