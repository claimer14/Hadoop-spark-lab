#!/bin/bash

# Функция для запуска Spark приложения
run_spark_app() {
    local app_name=$1
    local num_nodes=$2
    local is_optimized=$3
    
    echo "Запуск эксперимента: $app_name с $num_nodes DataNode (оптимизация: $is_optimized)"
    
    # Запуск Spark приложения
    docker exec spark /opt/spark/bin/spark-submit \
        --class "com.example.$app_name" \
        --master spark://spark:7077 \
        --executor-memory 1g \
        --driver-memory 1g \
        /app/target/scala-2.12/hadoop-lab_2.12-0.1.jar
    
    echo "Эксперимент завершен"
    echo "----------------------------------------"
}

# Сборка проекта
echo "Сборка проекта..."
docker exec spark sbt package

# Эксперимент 1: 1 DataNode, без оптимизации
run_spark_app "EmployeeAnalysis" 1 "нет"

# Эксперимент 2: 1 DataNode, с оптимизацией
run_spark_app "EmployeeAnalysisOpt" 1 "да"

# Эксперимент 3: 3 DataNode, без оптимизации
run_spark_app "EmployeeAnalysis" 3 "нет"

# Эксперимент 4: 3 DataNode, с оптимизацией
run_spark_app "EmployeeAnalysisOpt" 3 "да" 