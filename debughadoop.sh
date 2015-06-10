#!/usr/bin/env bash
export HADOOP_JOBTRACKER_OPTS="-Xdebug -Xrunjdwp:transport=dt_socket,address=8787,server=y,suspend=y"
bin/start-all.sh
