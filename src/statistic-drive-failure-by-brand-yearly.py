from pyspark.sql import SparkSession
from pathlib import Path
from pyspark.sql.functions import count, sum, first
from pyspark.sql.functions import when, col
from datetime import datetime, date
import csv
import os

def determine_date_format(date_str):
    if isinstance(date_str, date):
        return date_str

    try:
        # 尝试使用 %m/%d/%y 格式解析
        obj = datetime.strptime(date_str, '%m/%d/%y')
        return obj.date()
    except ValueError:
        # 如果抛出 ValueError，则尝试使用 xxxx 格式解析
        try:
            obj = datetime.strptime(date_str, 'xxxx')
            return obj.date()
        except ValueError:
            # 如果两种格式都不匹配，则返回 None 或其他标识
            return None

def traverse_files(directory,year):
    print(f"Directory: {directory}")
    path = Path(directory)
    # 直接列出顶层目录下的所有文件
    for item in path.iterdir():
        if item.is_file() and item.name.endswith('.csv'):
            #print(f"File: {item}")

            # 创建 SparkSession
            spark = SparkSession.builder \
                .appName("CSV File Processing") \
                .getOrCreate()

            # 读取 CSV 文件
            df = spark.read.csv(str(item), header=True, inferSchema=True)

            # 按照前缀构造新的列model_prefix
            df = df.withColumn(
                "model_prefix",
                when(col("model").startswith("CT"), "CT")
                .when(col("model").startswith("DELLBOSS"), "DELLBOSS")
                .when(col("model").startswith("HGST"), "HGST")
                .when((col("model").startswith("Seagate")) | (col("model").startswith("ST")), "Seagate")
                .when(col("model").startswith("TOSHIBA"), "TOSHIBA")
                .when(col("model").startswith("WDC"), "WDC")
                .otherwise("Others")
            )

            # 根据这个键来分组数据
            grouped_df = df.groupby('model_prefix').agg(
                count("*").alias("counts"),  # 计算每个组的行数
                sum("failure").alias("total_failures"),  # 计算每个组 failure 字段的总和
                first("date").alias("date_time")
            )

            # 显示分组结果
            grouped_df.show()

            # 注意：collect() 会将所有数据加载到驱动程序中，可能导致内存溢出
            rows = grouped_df.collect()
            # 打开（或创建）一个 CSV 文件，模式为写（'a'）
            with open('/Users/oliverliupeng/airflow/drive-failure-by-brand-' + str(year) + '.csv', mode='a', newline='') as file:
              writer = csv.writer(file)
              for row in rows:
                  model_prefix = row.model_prefix
                  counts = row.counts
                  total_failures = row.total_failures
                  date_time = determine_date_format(row.date_time)
                  year = date_time.year
                  month = date_time.month
                  day = date_time.day
                  print(f"date_time: {date_time}; model_prefix: {model_prefix}; total_failures: {total_failures}, counts: {counts}, year: {year}, month={month}, day={day}")

                  # 定义要写入的数据，每行数据是一个列表
                  data = [[date_time,model_prefix,counts,total_failures,year,month,day]]

                  # 写入多行数据
                  writer.writerows(data)

            print("分组数据已成功写入 csv 文件")

# 遍历天维度的csv文件，分别写到对应年份的中间文件中
local_directory = '/Users/oliverliupeng/airflow/backblaze-csv/'
years = ['2019', '2020', '2021', '2022', '2023']
quarters = ['Q1', 'Q2', 'Q3', 'Q4']
for year in years:
    # 拼接完整的文件路径
    target_filepath = os.path.join('/Users/oliverliupeng/airflow', 'drive-failure-by-brand-' + str(year) + '.csv')
    if os.path.isfile(target_filepath):
        print(f"The file '{target_filepath}' exists, no need to run")
    else:
        print(f"The file '{target_filepath}' does not exist, handling ...")
        # 打开（或创建）一个 CSV 文件，模式为写（'a'）
        with open('/Users/oliverliupeng/airflow/drive-failure-by-brand-' + str(year) + '.csv', mode='a', newline='') as file:
            writer = csv.writer(file)
            # 定义要写入的数据，每行数据是一个列表
            data = [['date_time','model_prefix','counts','total_failures','year','month','day']]
            writer.writerows(data)
        for quarter in quarters:
            if year == '2023' and quarter == 'Q4':
                break
            else:
                dir_name = 'data_' + quarter + "_" + year
                if year == '2022' and quarter == 'Q2':
                    traverse_files(local_directory + dir_name + '/' + dir_name + " /", year)
                elif int(year) >= 2022:
                    traverse_files(local_directory + dir_name + '/' + dir_name, year)
                else:
                    traverse_files(local_directory + dir_name, year)
