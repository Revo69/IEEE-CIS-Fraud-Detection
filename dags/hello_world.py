"""
Hello World DAG — проверочный пайплайн
=======================================
Цель: Убедиться, что Airflow 3.x правильно работает и подхватывает DAG'и.

Что делает этот DAG:
1. Печатает приветствие
2. Проверяет версии установленных библиотек (Pandas, Polars, DuckDB)
3. Делает простую операцию с DuckDB
4. Финальное сообщение об успехе

Как запустить:
- Поместите файл в папку dags/
- Откройте Airflow UI → http://localhost:8080
- Найдите DAG 'hello_world'
- Нажмите кнопку ▶ (Trigger DAG)
"""

from __future__ import annotations

from datetime import datetime, timedelta

from airflow.sdk import dag, task


# ============================================================
# Конфигурация DAG
# ============================================================
default_args = {
    "owner": "fraud-pipeline",
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
}


@dag(
    dag_id="hello_world",
    description="Проверочный DAG — убеждаемся, что Airflow и библиотеки работают",
    schedule=None,           # Запускаем вручную (не по расписанию)
    start_date=datetime(2026, 1, 1),
    catchup=False,           # Не запускать пропущенные runs
    default_args=default_args,
    tags=["test", "hello-world"],
)
def hello_world_dag():
    """
    Простой тестовый DAG для проверки окружения.
    Используем TaskFlow API (современный стиль Airflow 3.x).
    """

    # --------------------------------------------------------
    # Task 1: Приветствие
    # --------------------------------------------------------
    @task
    def say_hello() -> dict:
        """Первая задача — просто печатаем приветствие."""
        print("=" * 50)
        print("👋 Привет, Airflow 3.x!")
        print("   IEEE-CIS Fraud Detection Pipeline")
        print("=" * 50)

        return {
            "message": "Hello from Airflow!",
            "timestamp": datetime.now().isoformat(),
        }

    # --------------------------------------------------------
    # Task 2: Проверка версий библиотек
    # --------------------------------------------------------
    @task
    def check_libraries() -> dict:
        """
        Проверяем, что все нужные библиотеки установлены
        и показываем их версии.
        """
        import pandas as pd
        import polars as pl
        import duckdb
        import pyarrow as pa

        versions = {
            "pandas":  pd.__version__,
            "polars":  pl.__version__,
            "duckdb":  duckdb.__version__,
            "pyarrow": pa.__version__,
        }

        print("\n📦 Версии установленных библиотек:")
        print("-" * 35)
        for lib, version in versions.items():
            print(f"  {lib:<12} → {version}")
        print("-" * 35)

        return versions

    # --------------------------------------------------------
    # Task 3: Простая операция с DuckDB
    # --------------------------------------------------------
    @task
    def test_duckdb() -> dict:
        """
        Создаём маленькую таблицу в DuckDB и делаем запрос.
        Это имитирует то, что будем делать с реальными данными.
        """
        import duckdb

        # Создаём in-memory базу данных
        con = duckdb.connect()

        # Создаём тестовую таблицу — имитация транзакций
        con.execute("""
            CREATE TABLE test_transactions AS
            SELECT * FROM (VALUES
                (1, 'card_001', 150.50, 0),
                (2, 'card_002', 999.99, 1),
                (3, 'card_001',  45.00, 0),
                (4, 'card_003', 2500.00, 1),
                (5, 'card_002',  12.99, 0)
            ) AS t(TransactionID, CardID, Amount, isFraud)
        """)

        # Считаем статистику (как будем делать в реальном проекте)
        result = con.execute("""
            SELECT
                COUNT(*)                          AS total_transactions,
                SUM(isFraud)                      AS fraud_count,
                ROUND(AVG(Amount), 2)             AS avg_amount,
                ROUND(SUM(isFraud) * 100.0
                      / COUNT(*), 1)              AS fraud_rate_pct
            FROM test_transactions
        """).fetchone()

        stats = {
            "total_transactions": result[0],
            "fraud_count":        result[1],
            "avg_amount":         result[2],
            "fraud_rate_pct":     result[3],
        }

        print("\n🦆 DuckDB — тест прошёл успешно!")
        print("-" * 35)
        print(f"  Транзакций всего : {stats['total_transactions']}")
        print(f"  Мошеннических    : {stats['fraud_count']}")
        print(f"  Средняя сумма    : ${stats['avg_amount']}")
        print(f"  Доля фрода       : {stats['fraud_rate_pct']}%")
        print("-" * 35)

        con.close()
        return stats

    # --------------------------------------------------------
    # Task 4: Финальный отчёт
    # --------------------------------------------------------
    @task
    def final_report(greeting: dict, libraries: dict, db_stats: dict) -> None:
        """
        Собираем результаты всех задач и печатаем итоговый отчёт.
        Показывает, как задачи передают данные друг другу (XCom).
        """
        print("\n" + "=" * 50)
        print("✅ ВСЕ ЗАДАЧИ ВЫПОЛНЕНЫ УСПЕШНО!")
        print("=" * 50)
        print(f"\n  Время запуска : {greeting['timestamp']}")
        print(f"\n  Библиотеки    :")
        for lib, ver in libraries.items():
            print(f"    {lib:<12} {ver}")
        print(f"\n  DuckDB stats  :")
        print(f"    Транзакций   : {db_stats['total_transactions']}")
        print(f"    Фрод rate    : {db_stats['fraud_rate_pct']}%")
        print("\n  🚀 Окружение готово к работе!")
        print("=" * 50)

    # --------------------------------------------------------
    # Определяем порядок выполнения задач
    # --------------------------------------------------------
    greeting_result  = say_hello()
    library_versions = check_libraries()
    db_result        = test_duckdb()

    # final_report получает результаты всех трёх задач
    final_report(greeting_result, library_versions, db_result)


# Регистрируем DAG
hello_world_dag()
