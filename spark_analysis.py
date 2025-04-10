from pyspark.sql import SparkSession
import time
import logging
import psutil
import os
from pyspark.sql.functions import col, count, avg, sum, desc
import matplotlib.pyplot as plt
import pandas as pd

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    filename='spark_analysis.log'
)
logger = logging.getLogger('SparkAnalysis')

# Отключаем WARN и FATAL логи
logging.getLogger("org").setLevel(logging.ERROR)
logging.getLogger("akka").setLevel(logging.ERROR)

def get_memory_usage():
    """Получить текущее использование памяти процессом"""
    process = psutil.Process(os.getpid())
    return process.memory_info().rss / 1024 / 1024  # MB

def create_spark_session(app_name, master_url, is_optimized=False):
    """Создание SparkSession с оптимизациями или без"""
    builder = SparkSession.builder.appName(app_name).master(master_url)
    
    if is_optimized:
        builder = builder \
            .config("spark.executor.memory", "1g") \
            .config("spark.driver.memory", "1g") \
            .config("spark.sql.shuffle.partitions", "10") \
            .config("spark.default.parallelism", "10") \
            .config("spark.storage.memoryFraction", "0.8") \
            .config("spark.memory.offHeap.enabled", "true") \
            .config("spark.memory.offHeap.size", "512m")
    
    return builder.getOrCreate()

def analyze_data(spark, input_path, is_optimized=False):
    """Анализ данных с замером времени и использования памяти"""
    start_time = time.time()
    initial_memory = get_memory_usage()
    
    logger.info(f"Starting data analysis (Optimized: {is_optimized})")
    logger.info(f"Initial memory usage: {initial_memory:.2f} MB")
    
    logger.info("Reading data from HDFS")
    df = spark.read.csv(input_path, header=True, inferSchema=True)
    
    if is_optimized:

        logger.info("Applying optimizations")
        df = df.repartition(10).cache()
    
    logger.info("Performing analysis")
    
    # 1. Базовая статистика
    logger.info("Calculating basic statistics")
    df.describe().show()
    
    # 2. Подсчет сотрудников по департаментам и образованию
    dept_edu_counts = df.groupBy("Department", "education").count()
    logger.info("Department and education counts calculated")
    
    # 3. Средняя зарплата и производительность по департаментам
    dept_stats = df.groupBy("Department").agg(
        avg("Salary").alias("avg_salary"),
        avg("performance_score").alias("avg_performance"),
        sum("projects_completed").alias("total_projects")
    ).orderBy(desc("avg_salary"))
    logger.info("Department statistics calculated")
    
    # 4. Анализ менеджеров
    manager_stats = df.filter(col("is_manager") == True).groupBy("Department").agg(
        count("*").alias("manager_count"),
        avg("Salary").alias("avg_manager_salary")
    )
    logger.info("Manager statistics calculated")
    
 
    logger.info("\nDepartment and Education Distribution:")
    dept_edu_counts.show()
    
    logger.info("\nDepartment Statistics:")
    dept_stats.show()
    
    logger.info("\nManager Statistics:")
    manager_stats.show()
    
    if is_optimized:
        df.unpersist()
    
    end_time = time.time()
    final_memory = get_memory_usage()
    
    execution_time = end_time - start_time
    memory_used = final_memory - initial_memory
    
    logger.info(f"Analysis completed in {execution_time:.2f} seconds")
    logger.info(f"Memory usage: {memory_used:.2f} MB")
    
    return {
        'execution_time': execution_time,
        'memory_used': memory_used
    }

def plot_results(results):
    """Визуализация результатов экспериментов"""
    experiments = list(results.keys())
    execution_times = [results[exp]['execution_time'] for exp in experiments]
    memory_usage = [results[exp]['memory_used'] for exp in experiments]
    
    # Создаем два подграфика
    fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(15, 6))
    
    ax1.bar(experiments, execution_times)
    ax1.set_title('Время выполнения')
    ax1.set_ylabel('Секунды')
    ax1.tick_params(axis='x', rotation=45)
    
    ax2.bar(experiments, memory_usage)
    ax2.set_title('Использование памяти')
    ax2.set_ylabel('MB')
    ax2.tick_params(axis='x', rotation=45)
    
    plt.tight_layout()
    plt.savefig('experiment_results.png')
    logger.info("Results visualization saved as experiment_results.png")

def main():
    hdfs_path = "hdfs://namenode:8020/data/employee_dataset.csv"
    results = {}
    
    # Эксперимент 1: 1 DataNode, без оптимизаций
    logger.info("\n=== Experiment 1: 1 DataNode, No Optimization ===")
    spark = create_spark_session("EmployeeAnalysis-1DN", "spark://spark-master:7077")
    results['1DN-NoOpt'] = analyze_data(spark, hdfs_path)
    spark.stop()
    
    # Эксперимент 2: 1 DataNode, с оптимизациями
    logger.info("\n=== Experiment 2: 1 DataNode, Optimized ===")
    spark = create_spark_session("EmployeeAnalysis-1DN-Opt", "spark://spark-master:7077", True)
    results['1DN-Opt'] = analyze_data(spark, hdfs_path, True)
    spark.stop()
    
    # Эксперимент 3: 3 DataNode, без оптимизаций
    logger.info("\n=== Experiment 3: 3 DataNodes, No Optimization ===")
    spark = create_spark_session("EmployeeAnalysis-3DN", "spark://spark-master:7077")
    results['3DN-NoOpt'] = analyze_data(spark, hdfs_path)
    spark.stop()
    
    # Эксперимент 4: 3 DataNode, с оптимизациями
    logger.info("\n=== Experiment 4: 3 DataNodes, Optimized ===")
    spark = create_spark_session("EmployeeAnalysis-3DN-Opt", "spark://spark-master:7077", True)
    results['3DN-Opt'] = analyze_data(spark, hdfs_path, True)
    spark.stop()
    
    logger.info("\n=== Results Summary ===")
    for exp, metrics in results.items():
        logger.info(f"\n{exp}:")
        logger.info(f"Time: {metrics['execution_time']:.2f}s")
        logger.info(f"Memory: {metrics['memory_used']:.2f}MB")
    
    plot_results(results)

if __name__ == "__main__":
    main() 