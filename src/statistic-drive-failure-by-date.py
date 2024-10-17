from pyspark.sql import SparkSession
from pyspark.sql.functions import count
from pathlib import Path
import csv
import os

def traverse_files(directory):
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

            # 因为原始文件是按照天的维度统计的，所以此处直接聚合整个文件的数据即可
            sum_failure = df.agg({"failure": "sum"}).collect()[0][0]
            sum_drive = df.agg({"serial_number": "count"}).collect()[0][0]
            print(f"date: {item.name[0:-4]}; Sum failure: {sum_failure}; drive count: {sum_drive}")

            # 定义要写入的数据，每行数据是一个列表
            data = [[item.name[0:-4],sum_drive,sum_failure]]

            # 打开（或创建）一个 CSV 文件，模式为写（'a'）
            with open('/Users/oliverliupeng/airflow/drive-failure-by-date.csv', mode='a', newline='') as file:
                writer = csv.writer(file)

                # 写入多行数据
                writer.writerows(data)

            print("数据已成功写入 csv 文件")


local_filepath = os.path.join('/Users/oliverliupeng/airflow', 'drive-failure-by-date.csv')
# 文件存在则不重复下载
if os.path.isfile(local_filepath):
    print(f"The file '{local_filepath}' exists, no need to run statistic-drive-failure-by-date.py")
else:
    print(f"The file '{local_filepath}' does not exist, running statistic-drive-failure-by-date.py ...")
    # 遍历指定的解压后的csv目录, 统一按照天维度写入指定文件
    local_directory = '/Users/oliverliupeng/airflow/backblaze-csv/'
    years = ['2019', '2020', '2021', '2022', '2023']
    quarters = ['Q1', 'Q2', 'Q3', 'Q4']
    for year in years:
        for quarter in quarters:
            if year == '2023' and quarter == 'Q4':
                break
            else:
                dir_name = 'data_' + quarter + "_" + year
                # 22年Q2的官方目录解压格式与其他的不一致，所以需要特殊处理
                if year == '2022' and quarter == 'Q2':
                    traverse_files(local_directory + dir_name + '/' + dir_name + " /")
                # 22年以后的zip文件解压后会多一个目录，所以需要特殊处理
                elif int(year) >= 2022:
                    traverse_files(local_directory + dir_name + '/' + dir_name)
                else:
                    traverse_files(local_directory + dir_name)
