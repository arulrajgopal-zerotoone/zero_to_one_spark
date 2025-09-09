
spark.conf.set("fs.azure.account.key.<your_storage_account_name>.dfs.core.windows.net",<your_account_name>)


spark.conf.set("spark.sql.autoBroadcastJoinThreshold", 100 * 1024 * 1024) // 100 MB
spark.conf.set("spark.sql.broadcastTimeout", "600s")


spark.conf.set("spark.sql.adaptive.enabled", "false")
spark.conf.set("spark.sql.shuffle.partitions", 10)

