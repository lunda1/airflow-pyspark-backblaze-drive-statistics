import os
import requests
import sys
import zipfile

def download_file(url_prefix, local_directory, filename=None):
    url = url_prefix + filename
    # 如果未指定文件名，则从URL中提取文件名
    if filename is None:
        filename = os.path.basename(url)

    # 确保本地目录存在
    if not os.path.exists(local_directory):
        os.makedirs(local_directory)

    # 拼接完整的文件路径
    local_filepath = os.path.join(local_directory, filename)

    # 文件存在则不重复下载
    if os.path.isfile(local_filepath):
        print(f"The file '{local_filepath}' exists, no need to download")
    else:  
        print(f"The file '{local_filepath}' does not exist, downloading ...")
        try:
            # 发送HTTP GET请求
            response = requests.get(url, stream=True)
            response.raise_for_status()  # 检查请求是否成功

            with open(local_filepath, 'wb') as f:
                for chunk in response.iter_content(chunk_size=8192):
                    f.write(chunk)

            print(f"文件已成功下载到 {local_filepath}")
            
        except requests.RequestException as e:
            print(f"下载文件时出错: {e}")


def unzip_file(zip_path, extract_to):  
    # 确保解压目录存在，如果不存在则创建，并且解压；存在则不重复解压  
    if not os.path.exists(extract_to):  
        os.makedirs(extract_to)  
      
        # 打开ZIP文件  
        with zipfile.ZipFile(zip_path, 'r') as zip_ref:  
            # 解压所有文件到指定目录  
            zip_ref.extractall(extract_to)  
      
        print(f"文件已成功解压到 {extract_to}")
    else:
        print(f"The directory '{extract_to}' exists, no need to unzip")


# 示例用法
url_prefix = 'https://f001.backblazeb2.com/file/Backblaze-Hard-Drive-Data/'
local_directory = '/Users/oliverliupeng/airflow/backblaze'
# 你可以指定文件名，也可以不指定（不指定时会从URL中提取）
# filename = 'custom_filename.zip'
years = ['2019', '2020', '2021', '2022', '2023']
quarters = ['Q1', 'Q2', 'Q3', 'Q4']  
for year in years:
    for quarter in quarters:
        if year == '2023' and quarter == 'Q4':
            break
        else:
            filename = 'data_' + quarter + "_" + year
            full_filename = filename + ".zip"
            download_file(url_prefix, local_directory, full_filename)
            # 调用函数，传入ZIP文件路径和解压到的目录路径  
            unzip_file(local_directory + "/" + full_filename, local_directory + "-csv" + "/" + filename)
#sys.exit(0)
