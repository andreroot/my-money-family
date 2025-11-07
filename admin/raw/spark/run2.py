from pyspark.sql import SparkSession

ano = "2025"  # Substitua pelo ano desejado
input_path = f"./output/credito_{ano}.csv"
output_path = f"./output/credito_{ano}.parquet"

spark = SparkSession.builder.appName("CSVtoParquet").getOrCreate()

# Leia o CSV
df = spark.read.csv(input_path, quotechar='"', encoding='utf-8',header=True, sep=";", inferSchema=True)

# Salve como Parquet
df.write.parquet(output_path)

spark.stop()


# aws emr add-steps \
#     --cluster-id j-1MN5SCT7WUO8C \
#     --steps Type=Spark,Name="TesteSpark",ActionOnFailure=CONTINUE,\
# Args=[--deploy-mode,cluster,--master,yarn,--conf,spark.yarn.submit.waitAppCompletion=true,--py-files,s3://poc-file-system-emr-log-30102025/execute-spark/run.py,s3://poc-file-system-emr-log-30102025/execute-spark/run.py]


# # aws emr list-steps --cluster-id j-1MN5SCT7WUO8C