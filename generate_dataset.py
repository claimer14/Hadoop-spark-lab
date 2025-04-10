import pandas as pd
import numpy as np
from datetime import datetime, timedelta

np.random.seed(42)
n_rows = 150000

data = {

    'age': np.random.randint(18, 80, n_rows),
    'salary': np.random.normal(50000, 15000, n_rows).round(2),
    'experience': np.random.exponential(5, n_rows).round(1),
    
    # Категориальные признаки
    'department': np.random.choice(['IT', 'HR', 'Finance', 'Marketing', 'Sales'], n_rows),
    'education': np.random.choice(['Bachelor', 'Master', 'PhD', 'High School'], n_rows),
    
    # Дата и время
    'hire_date': [datetime.now() - timedelta(days=np.random.randint(0, 3650)) for _ in range(n_rows)],
    
    # Булево значение
    'is_manager': np.random.choice([True, False], n_rows, p=[0.2, 0.8]),
    
    # Дополнительные числовые признаки
    'projects_completed': np.random.poisson(5, n_rows),
    'performance_score': np.random.uniform(0, 100, n_rows).round(1)
}

df = pd.DataFrame(data)

df.to_csv('employee_dataset.csv', index=False)
print(f"Датасет создан: {n_rows} строк, {len(df.columns)} признаков")
print("\nТипы данных:")
print(df.dtypes) 