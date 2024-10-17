## 操作流程概述

1. 本机 Docker 部署 airflow
```bash
https://www.ygtq.cc/use-docker-to-quickly-deploy-airflow/
```

2. 修改 airflow 的 docker-compose.yaml 中的挂载目录
```bash
x-airflow-common:
  &airflow-common
  # In order to add custom dependencies or upgrade provider packages you can use your extended image.
  # Comment the image line, place your Dockerfile in the directory where you placed the docker-compose.yaml
  # and uncomment the "build" line below, Then run `docker-compose build` to build the images.
  image: ${AIRFLOW_IMAGE_NAME:-apache/airflow:2.7.0}
  # build: .
  environment:
    &airflow-common-env
  volumes:
    - ${AIRFLOW_PROJ_DIR:-.}/dags:/opt/airflow/dags
    - ${AIRFLOW_PROJ_DIR:-.}/logs:/opt/airflow/logs
    - ${AIRFLOW_PROJ_DIR:-.}/config:/opt/airflow/config
    - ${AIRFLOW_PROJ_DIR:-.}/plugins:/opt/airflow/plugins
    - /Users/oliverliupeng/airflow/backblaze:/Users/oliverliupeng/airflow/backblaze
    - /Users/oliverliupeng/airflow:/Users/oliverliupeng/airflow
```

3. airflow 的 worker 对应 docker 安装 pyspark
```bash
pip3 install pyspark
```

4. 编写 dag 文件并放到对应目录
```bash
drive_statistics_dag.py
```

5. 编写 任务执行脚本并放到指定目录
```bash
hello.py
statistic-drive-failure-by-brand-whole.py
statistic-drive-failure-by-brand-yearly.py
statistic-drive-failure-by-date.py
```

6. 启动 airflow 并进入控制台触发 dag
```bash
http://localhost:8080/home
```

7. 产出执行结果csv文件
```bash
drive-failure-by-brand.csv
drive-failure-by-date.csv
```

