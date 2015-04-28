#!/usr/bin/env bash
stop-dfs.sh
rm -r logs
rm -r meta
rm -r tmp/*
ant clean
ant
cp build/hadoop-core-1.0.4-SNAPSHOT.jar  ./hadoop-core-1.0.4.jar
hadoop namenode -format
