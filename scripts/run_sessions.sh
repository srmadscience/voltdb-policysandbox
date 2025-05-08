#!/bin/sh

. $HOME/.profile


TOTALRAM=`cat /proc/meminfo | grep MemTotal | awk '{ print $2 }'`
QUARTERRAM=`expr ${TOTALRAM} / 4 ` 
THREEQUARTERRAM=`expr ${QUARTERRAM} \* 3`
THREEQUARTERRAMBYTES=`expr ${THREEQUARTERRAM} \* 1024`
echo RAM is ${THREEQUARTERRAMBYTES}

if 
	[ "$#" -ne 5 ]
then

	echo "Using default parameters"

	USERCOUNT=1000000
	TPMS=20
	DURATIONSECONDS=7200
	CELLCOUNT=2
	OFFSET=0

else
	USERCOUNT=$1
	TPMS=$2
	DURATIONSECONDS=$3
	CELLCOUNT=$4
	OFFSET=$5
fi


cd
mkdir logs 2> /dev/null

cd voltdb-policysandbox/jars 

java ${JVMOPTS} -Xmx${THREEQUARTERRAMBYTES} -jar voltdb-policysandbox-client.jar `cat $HOME/.vdbhostnames`  $USERCOUNT $TPMS $DURATIONSECONDS $CELLCOUNT $OFFSET | tee -a $HOME/logs/sessions.log
