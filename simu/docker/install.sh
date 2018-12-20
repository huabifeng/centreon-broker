#!/bin/bash

mkdir -p /usr/share/centreon-broker/lua/neb
mkdir -p /etc/centreon-broker
mkdir -p /var/log/centreon-broker
mkdir -p /var/lib/centreon-broker

rsync -avzt lua /usr/share/centreon-broker/
cp etc/* /etc/centreon-broker/
