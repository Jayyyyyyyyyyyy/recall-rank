#!/usr/bin/env bash
# Created by liujikun
output=/liujikun/samples.7days.train
hadoop fs -mkdir $output
hadoop fs -rmr $output/*
spark-submit \
--class com.wifi.recommend.RecallRank \
--master yarn-client \
--num-executors 100 \
--conf spark.kryo.referenceTracking=false \
--conf spark.kryoserializer.buffer=64k \
--conf spark.kryoserializer.buffer.max=600m \
--conf spark.serializer=org.apache.spark.serializer.KryoSerializer \
--conf spark.network.timeout=3600 \
--conf spark.dynamicAllocation.minExecutors=10 \
--conf spark.default.parallelism=200 \
--conf spark.files.maxPartitionBytes=13421772 \
--conf spark.driver.maxResultSize=10g \
--conf spark.executor.memory=22g \
--conf spark.executor.cores=23 \
--conf spark.cores.max=1000 \
./target/recall-rank-1.0-SNAPSHOT.jar /hudelu/feature_table/ $output 50
