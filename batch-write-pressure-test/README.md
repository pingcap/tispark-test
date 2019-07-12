# TiSpark Batch Write Pressure Test

bin/spark-submit \
--jars tispark-core-2.2.0-SNAPSHOT-jar-with-dependencies.jar \
--class com.pingcap.tispark.BatchWritePressureTest \
--executor-memory 16g \
--executor-cores 4 \
--total-executor-cores 120 \
--conf "spark.executor.extraJavaOptions=-XX:+UseG1GC" \
batch-write-pressure-test-1.0.0-SNAPSHOT.jar \
$path $$tidbIP $tidbPort $pdAddr $database $table $skipCommitSecondaryKey $lockTTLSeconds $writeConcurrency

