from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago
from datetime import timedelta

# 定义DAG的参数
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'execution_timeout': timedelta(hours=1),  # 设置DAG的超时时间为1小时
    'start_date': days_ago(2),  # 从两天前开始
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,  # 重试次数
    'retry_delay': timedelta(minutes=5),  # 重试间隔
}

# 创建DAG对象
dag = DAG(
    'backblaze_drive_statistics_dag',
    default_args=default_args,
    description='A DAG to execute a Python script periodically',
    schedule_interval=timedelta(days=1),  # 设置定时触发器，每天执行一次
    catchup=False,  # 不补跑错过的任务
)

# 定义下载和解压zip文件的Python函数
def download_and_unzip():
    # 这里可以添加执行Python脚本的逻辑
    # 例如，使用subprocess模块来调用外部脚本
    import subprocess
    result = subprocess.run(['python3', '/Users/oliverliupeng/airflow/hello.py'], capture_output=True, text=True)
    print(result.stdout)
    if result.returncode != 0:
        raise Exception(f"Python script failed with return code {result.returncode}: {result.stderr}")

task1_download_and_unzip = PythonOperator(
    task_id='task1_download_and_unzip',
    provide_context=True,
    python_callable=download_and_unzip,
    dag=dag,
)

# 定义统计按天的数据
def statistics_by_date():
    # 这里可以添加执行Python脚本的逻辑
    # 例如，使用subprocess模块来调用外部脚本
    import subprocess
    result = subprocess.run(['python3', '/Users/oliverliupeng/airflow/statistic-drive-failure-by-date.py'], capture_output=True, text=True)
    print(result.stdout)
    if result.returncode != 0:
        raise Exception(f"Python script failed with return code {result.returncode}: {result.stderr}")

task2_statistics_by_date = PythonOperator(
    task_id='task2_statistics_by_date',
    provide_context=True,
    python_callable=statistics_by_date,
    dag=dag,
)

# 定义统计按年的数据（按照年份的独立数据）
def statistics_by_brand_and_yearly():
    # 这里可以添加执行Python脚本的逻辑
    # 例如，使用subprocess模块来调用外部脚本
    import subprocess
    result = subprocess.run(['python3', '/Users/oliverliupeng/airflow/statistic-drive-failure-by-brand-yearly.py'], capture_output=True, text=True)
    print(result.stdout)
    if result.returncode != 0:
        raise Exception(f"Python script failed with return code {result.returncode}: {result.stderr}")


task3_statistics_by_brand_and_yearly = PythonOperator(
    task_id='task3_statistics_by_brand_and_yearly',
    provide_context=True,
    python_callable=statistics_by_brand_and_yearly,
    dag=dag,
)

# 定义统计按年的数据（汇总到一个文件）
def statistics_by_brand_and_yearly_whole():
    # 这里可以添加执行Python脚本的逻辑
    # 例如，使用subprocess模块来调用外部脚本
    import subprocess
    result = subprocess.run(['python3', '/Users/oliverliupeng/airflow/statistic-drive-failure-by-brand-whole.py'], capture_output=True, text=True)
    print(result.stdout)
    if result.returncode != 0:
        raise Exception(f"Python script failed with return code {result.returncode}: {result.stderr}")


task4_statistics_by_brand_and_yearly_whole = PythonOperator(
    task_id='task4_statistics_by_brand_and_yearly_whole',
    provide_context=True,
    python_callable=statistics_by_brand_and_yearly_whole,
    dag=dag,
)


task1_download_and_unzip >> task2_statistics_by_date >> task3_statistics_by_brand_and_yearly >> task4_statistics_by_brand_and_yearly_whole

