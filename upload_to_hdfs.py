import subprocess
import os

def upload_to_hdfs():
    
    local_file = 'employee_dataset.csv'
    
    # Путь в HDFS
    hdfs_path = '/data/employee_dataset.csv'
    
    print("Создание директории в HDFS...")
    subprocess.run(['docker', 'exec', 'namenode', 'hdfs', 'dfs', '-mkdir', '-p', '/data'])
    
    print(f"Копирование {local_file} в контейнер...")
    subprocess.run(['docker', 'cp', local_file, f'namenode:/{local_file}'])
    
    print(f"Загрузка файла в HDFS...")
    subprocess.run(['docker', 'exec', 'namenode', 'hdfs', 'dfs', '-put', f'/{local_file}', hdfs_path])

    print("Проверка загруженного файла...")
    subprocess.run(['docker', 'exec', 'namenode', 'hdfs', 'dfs', '-ls', '/data'])

if __name__ == "__main__":
    upload_to_hdfs() 