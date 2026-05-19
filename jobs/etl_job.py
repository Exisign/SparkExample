# /opt/jobs/etl_job.py
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql import functions as F
from pyspark.sql.window import Window


# 2. SparkSession 획득 혹은 생성 (클러스터 연결)
spark = SparkSession.builder\
.appName('Hospital_ETL_Cluster')\
.master('spark://spark-master:7077')\
.config("spark.executor.memory", "512M")\
.config("spark.executor.cores", "1")\
.config("spark.sql.shuffle.partitions", "8")\
.getOrCreate()

# 로그 레벨 지정
spark.sparkContext.setLogLevel("WARN")

print("\n" + "-" * 50)
print("클러스터 연결 정보")
print("-" * 50)
print(f" Master:{ spark.sparkContext.master}")
print(f" App ID:{ spark.sparkContext.applicationId}")
print(f" App Name:{ spark.sparkContext.appName}")
print("-" * 50 + "\n")


# 10. 스파크 종료(세션 종료)
spark.stop()
print("\n" + "-" * 50)
print("스파크 종료(세션 종료)")
print("-" * 50 + "\n")
