import os
from celery import Celery

try:
    with open("/public/.master", "r") as fh:
        master_hostname = fh.read().rstrip()
except IOError:
    master_hostname = 'localhost'

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
