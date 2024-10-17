from pyspark.sql import SparkSession
from pathlib import Path
from pyspark.sql.functions import count, sum, first
from pyspark.sql.functions import when, col
from datetime import datetime, date
import csv
import os

def statistic_failures(directory):
    # 创建 SparkSession
    spark = SparkSession.builder \
        .appName("CSV File Processing") \
        .getOrCreate()

    # 读取 CSV 文件
    df = spark.read.csv(directory, header=True, inferSchema=True)

    # 根据这个键来分组数据
    grouped_df = df.groupby('model_prefix').agg(
        count("*").alias("counts"),  # 计算每个组的行数
        sum("total_failures").alias("total_failures"),  # 计算每个组 failure 字段的总和
        first("year").alias("year")
    )

    # 显示分组结果
    grouped_df.show()

    # 注意：collect() 会将所有数据加载到驱动程序中，可能导致内存溢出
    rows = grouped_df.collect()
    # 打开（或创建）一个 CSV 文件，模式为写（'w'）
    with open('/Users/oliverliupeng/airflow/drive-failure-by-brand.csv', mode='a', newline='') as file:
        writer = csv.writer(file)
        for row in rows:
            model_prefix = row.model_prefix
            counts = row.counts
            total_failures = row.total_failures
            year = row.year
            print(f"model_prefix: {model_prefix}; total_failures: {total_failures}, counts: {counts}, year: {year}")

            # 定义要写入的数据，每行数据是一个列表
            data = [[model_prefix,counts,total_failures,year]]

            # 写入多行数据
            writer.writerows(data)

    print("分组数据已成功写入 csv 文件")


# 拼接完整的文件路径
target_filepath = os.path.join('/Users/oliverliupeng/airflow', 'drive-failure-by-brand.csv')
if os.path.isfile(target_filepath):
    print(f"The file '{target_filepath}' exists, no need to run")
else:
    print(f"The file '{target_filepath}' does not exist, handling ...")

    # 遍历yearly统计数据，并汇总
    local_directory = '/Users/oliverliupeng/airflow/'
    years = ['2019', '2020', '2021', '2022', '2023']
    # 写入表头
    with open('/Users/oliverliupeng/airflow/drive-failure-by-brand.csv', mode='a', newline='') as file:
        writer = csv.writer(file)
        # 定义要写入的数据，每行数据是一个列表
        data = [['model_prefix','counts','total_failures','year']]
        # 写入多行数据
        writer.writerows(data)
    for year in years:
        dir_name = 'drive-failure-by-brand-' + year + '.csv'
        statistic_failures(local_directory + dir_name)
