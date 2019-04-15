#!/bin/bash

rep=$1
if [[ -z $rep ]] ; then
  echo "This script needs an argument that is how many times to repeat the"
  echo "test program."
  exit 1
fi

for ((i=0; i < rep ; i++ )) ; do
  echo "########################"
  echo "#          $i          #"
  echo "########################"
  echo
  rm -rf /var/lib/centreon/metrics
  rm -rf /var/lib/centreon/status
  /usr/sbin/cbd /etc/centreon-broker/central-broker.xml
done
