#!/bin/bash

MOUNT=$(which mount)
SYSTEMCTL=$(which systemctl)

mkdir -p /public

mkdir -p /var/run/celery
chown -R ubuntu:ubuntu /var/run/celery

$MOUNT -t nfs {{ master_ip }}:/public /public

$SYSTEMCTL restart celery.service
