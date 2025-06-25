#!/bin/sh

cd /home/ubuntu
. ./.profile

cd voltdb-policysandbox/scripts
$HOME/bin/reload_dashboards.sh Policy.json

sqlcmd --servers=vdb1 < ../ddl/voltdb-policysandbox-createDB.sql
java ${JVMOPTS} -jar $HOME/bin/addtodeploymentdotxml.jar vdb1 deployment topics.xml
