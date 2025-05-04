#!/bin/sh

. $HOME/.profile


if 
	[ "$#" -ne 5 ]
then

	echo "Using default parameters"

	USERCOUNT=2000000
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

java ${JVMOPTS} -jar voltdb-policysandbox-client.jar `cat $HOME/.vdbhostnames`  $USERCOUNT $TPMS $DURATIONSECONDS $CELLCOUNT $OFFSET | tee -a $HOME/logs/sessions.log
