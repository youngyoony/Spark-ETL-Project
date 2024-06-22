#!/bin/bash

JARS=""

JOBNAME="DataAnalysis"
shift 1
SCRIPT=$@
echo ${SCRIPT}

docker exec -it de-2024-spark-master-1 spark-submit \
  --name ${JOBNAME} \
  --master spark://spark-master:7077 \
  --jars ${JARS} \
  --conf spark.eventLog.enabled=true \
  --conf spark.eventLog.dir={} \
  --conf spark.dynamicAllocation.enabled=true \
  --conf spark.dynamicAllocation.executorIdleTimeout=2m \
  --conf spark.dynamicAllocation.minExecutors=1 \
  --conf spark.dynamicAllocation.maxExecutors=6 \
  --conf spark.dynamicAllocation.initialExecutors=2 \
  --conf spark.memory.offHeap.enabled=true \
  --conf spark.memory.offHeap.size=4G \
  --conf spark.shuffle.service.enabled=true \
  --conf spark.sql.shuffle.partitions=200 \
  --conf spark.rpc.io.serverThreads=64 \
  --conf spark.executor.memory=4G \
  --conf spark.driver.memory=4G \
  --conf spark.driver.maxResultSize=2G \
  --conf spark.hadoop.mapreduce.output.fileoutputformat.compress=false \
  --num-executors 4 \
  --executor-cores 2 \
  ${SCRIPT}
