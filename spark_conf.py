
spark.conf.set("fs.azure.account.key.<your_storage_account_name>.dfs.core.windows.net",<your_account_name>)


spark.conf.set("spark.sql.autoBroadcastJoinThreshold", 100 * 1024 * 1024) // 100 MB
spark.conf.set("spark.sql.broadcastTimeout", "600s")


spark.conf.set("spark.sql.adaptive.enabled", "false")
spark.conf.set("spark.sql.shuffle.partitions", 10)
spark.conf.set("spark.sql.files.maxPartitionBytes", "1g")

spark.conf.set("spark.executor.memory", "4g") #  Usage: Sets the amount of memory to allocate per Spark executor.
spark.conf.set("spark.shuffle.file.buffer", "64k") # Usage: Specifies the buffer size for reading and writing shuffle files.
spark.conf.set("spark.task.maxFailures", "5") # Usage: Defines the maximum number of failures allowed for a single task before the whole job is considered failed.
spark.conf.set("spark.reducer.maxSizeInFlight", "96m") # Usage: Sets the maximum size of map output data to fetch simultaneously from each reduce task.
spark.conf.set("spark.databricks.delta.autoCompact", "true") # Usage: Enables automatic compaction of the Delta table files for better performance.
spark.conf.set("spark.databricks.delta.retentionDurationCheck.enabled", "true") # Usage: Enables or disables retention duration checks during Delta table compaction.
spark.conf.set("spark.databricks.io.cache.enabled", "true") # Usage: Enables or disables caching for improved I/O performance.
