#!/bin/bash

CURL=$(which curl)
SYSTEMCTL=$(which systemctl)

VPC_CIDR_BLOCK=$($CURL "http://169.254.169.254/latest/meta-data/network/interfaces/macs/$($CURL http://169.254.169.254/latest/meta-data/network/interfaces/macs/ 2>/dev/null | head -n 1)vpc-ipv4-cidr-block")

echo "/public $VPC_CIDR_BLOCK(rw,sync,no_subtree_check)" > /etc/exports

$SYSTEMCTL restart nfs-server.service

echo $($CURL http://169.254.169.254/latest/meta-data/local-ipv4) > /public/.master

chown -R ubuntu:ubuntu /public/
