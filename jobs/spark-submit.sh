#!/bin/bash

JARS="/opt/bitnami/spark/resources/elasticsearch-spark-30_2.12-8.4.3.jar"

JOBNAME="RefinePipeline"
SCRIPT=$@
echo ${SCRIPT}

docker exec -it de-2024-spark-master-1 spark-submit \
  --name ${JOBNAME} \
  --master spark://spark-master:7077 \
  --jars ${JARS} \
  --conf spark.dynamicAllocation.enabled=true \
  --conf spark.dynamicAllocation.executorIdleTimeout=2m \
  --conf spark.dynamicAllocation.minExecutors=1 \
  --conf spark.dynamicAllocation.maxExecutors=2 \
  --conf spark.dynamicAllocation.initialExecutors=2 \
  --conf spark.memory.offHeap.enabled=true \
  --conf spark.memory.offHeap.size=4G \
  --conf spark.shuffle.service.enabled=true \
  --conf spark.sql.shuffle.partitions=200 \
  --conf spark.rpc.io.serverThreads=64 \
  --conf spark.executor.memory=4G \
  --conf spark.driver.memory=4G \
  --conf spark.driver.maxResultSize=2G \
  --num-executors 2 \
  --executor-cores 1 \
  ${SCRIPT}
