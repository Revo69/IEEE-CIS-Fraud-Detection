"""
MLflow: Baseline Fraud Detection Model
========================================
Цель этого скрипта — не построить идеальную модель,
а показать как data engineer логирует эксперименты.

Что делаем:
1. Загружаем данные из Parquet (результат нашего Transform шага)
2. Готовим features (выбираем числовые колонки)
3. Обучаем простую LogisticRegression как baseline
4. Логируем всё в MLflow:
   - параметры модели
   - метрики качества
   - саму модель как артефакт
5. Запускаем второй эксперимент с другими параметрами
   чтобы показать сравнение в MLflow UI

Запуск:
    python src/ml/train.py

MLflow UI:
    mlflow ui
    Открыть: http://localhost:5000
"""

from __future__ import annotations

import sys
import os
from pathlib import Path

import pandas as pd
import numpy as np
import mlflow
import mlflow.sklearn
from sklearn.linear_model import LogisticRegression
from sklearn.model_selection import train_test_split
from sklearn.preprocessing import StandardScaler
from sklearn.metrics import (
    accuracy_score,
    precision_score,
    recall_score,
    f1_score,
    roc_auc_score,
    classification_report,
)

# Добавляем корень проекта в путь
ROOT_DIR = Path(__file__).parent.parent.parent
sys.path.insert(0, str(ROOT_DIR))

from src.utils.logger import logger


# ============================================================
# Конфигурация
# ============================================================

# Путь к Parquet файлу (результат Transform шага)
PARQUET_PATH = ROOT_DIR / "data" / "processed" / "parquet" / "staging_fraud.parquet"

# MLflow эксперимент
EXPERIMENT_NAME = "ieee-cis-fraud-detection"

# Колонки которые используем как features
# Выбираем только числовые и те что точно есть после трансформации
FEATURE_COLUMNS = [
    "TransactionAmt",
    "log_TransactionAmt",
    "card1",
    "card2",
    "card5",
    "addr1",
    "dist1",
    "C1", "C2", "C6", "C11",
    "D1", "D10", "D15",
    "tx_hour",
    "tx_day_of_week",
    "is_night_tx",
    "is_weekend",
    "is_large_tx",
    "has_identity",
    "card1_tx_count",
    "card1_avg_amt",
]

TARGET_COLUMN = "isFraud"


# ============================================================
# Загрузка и подготовка данных
# ============================================================

def load_data() -> tuple[pd.DataFrame, pd.Series]:
    """
    Загружаем данные из Parquet и готовим для обучения.

    Returns:
        X — матрица признаков
        y — целевая переменная (isFraud)
    """
    logger.info(f"Загружаем данные из: {PARQUET_PATH}")

    if not PARQUET_PATH.exists():
        raise FileNotFoundError(
            f"Parquet файл не найден: {PARQUET_PATH}\n"
            f"Сначала запустите Airflow DAG: transform_data"
        )

    df = pd.read_parquet(PARQUET_PATH)
    logger.info(f"  Загружено строк : {len(df):,}")
    logger.info(f"  Колонок всего   : {len(df.columns)}")

    # Берём только нужные колонки
    available_features = [c for c in FEATURE_COLUMNS if c in df.columns]
    missing = set(FEATURE_COLUMNS) - set(available_features)
    if missing:
        logger.warning(f"  Отсутствуют колонки: {missing}")

    X = df[available_features].copy()
    y = df[TARGET_COLUMN].copy()

    # Заменяем оставшиеся NaN на -999
    X = X.fillna(-999)

    logger.info(f"  Features        : {len(available_features)}")
    logger.info(f"  Fraud rate      : {y.mean()*100:.2f}%")

    return X, y


# ============================================================
# Обучение и логирование в MLflow
# ============================================================

def train_and_log(
    X_train: pd.DataFrame,
    X_test: pd.DataFrame,
    y_train: pd.Series,
    y_test: pd.Series,
    params: dict,
    run_name: str,
) -> dict:
    """
    Обучает модель и логирует всё в MLflow.

    Это ключевая функция — показывает паттерн логирования:
    mlflow.start_run() → log_params() → log_metrics() → log_model()

    Args:
        params: гиперпараметры модели для логирования
        run_name: имя запуска в MLflow UI

    Returns:
        Словарь с метриками
    """
    with mlflow.start_run(run_name=run_name):

        # ------ Логируем параметры ------
        # Параметры — это настройки модели (что мы ПОДАЁМ)
        mlflow.log_params(params)
        mlflow.log_param("features_count", X_train.shape[1])
        mlflow.log_param("train_size", len(X_train))
        mlflow.log_param("test_size", len(X_test))
        mlflow.log_param("fraud_rate_train", round(y_train.mean(), 4))

        # ------ Масштабирование ------
        scaler = StandardScaler()
        X_train_scaled = scaler.fit_transform(X_train)
        X_test_scaled  = scaler.transform(X_test)

        # ------ Обучение ------
        logger.info(f"  Обучаем модель: {run_name}...")
        model = LogisticRegression(
            C=params["C"],
            max_iter=params["max_iter"],
            class_weight=params.get("class_weight", None),
            random_state=42,
            n_jobs=-1,
        )
        model.fit(X_train_scaled, y_train)

        # ------ Предсказания ------
        y_pred      = model.predict(X_test_scaled)
        y_pred_prob = model.predict_proba(X_test_scaled)[:, 1]

        # ------ Метрики ------
        # Метрики — это результаты (что мы ПОЛУЧАЕМ)
        metrics = {
            "accuracy":  round(accuracy_score(y_test, y_pred), 4),
            "precision": round(precision_score(y_test, y_pred, zero_division=0), 4),
            "recall":    round(recall_score(y_test, y_pred, zero_division=0), 4),
            "f1":        round(f1_score(y_test, y_pred, zero_division=0), 4),
            "roc_auc":   round(roc_auc_score(y_test, y_pred_prob), 4),
        }

        # Логируем метрики в MLflow
        mlflow.log_metrics(metrics)

        # ------ Логируем модель ------
        # Модель сохраняется как артефакт — её можно скачать и использовать
        mlflow.sklearn.log_model(
            sk_model=model,
            artifact_path="model",
            registered_model_name="fraud_detection_baseline",
        )

        # ------ Логируем feature importance ------
        # Для линейной модели — абсолютные значения коэффициентов
        feature_importance = pd.DataFrame({
            "feature":    X_train.columns,
            "importance": np.abs(model.coef_[0]),
        }).sort_values("importance", ascending=False)

        # Сохраняем как CSV артефакт
        import tempfile
        importance_path = Path(tempfile.gettempdir()) / "feature_importance.csv"
        feature_importance.to_csv(importance_path, index=False)
        mlflow.log_artifact(str(importance_path), "feature_analysis")

        # ------ Вывод результатов ------
        logger.info(f"\n  Результаты [{run_name}]:")
        logger.info(f"  {'─' * 35}")
        for metric, value in metrics.items():
            logger.info(f"  {metric:<12} : {value:.4f}")
        logger.info(f"  {'─' * 35}")

        logger.info(f"\n  Топ-5 важных признаков:")
        for _, row in feature_importance.head(5).iterrows():
            logger.info(f"    {row['feature']:<25} {row['importance']:.4f}")

        return metrics


# ============================================================
# Главная функция
# ============================================================

def main():
    logger.info("=" * 55)
    logger.info("  MLFLOW: Запуск экспериментов")
    logger.info("=" * 55)

    # 1. Загружаем данные
    X, y = load_data()

    # 2. Разбиваем на train/test
    # stratify=y — сохраняем пропорцию фрода в обоих наборах
    X_train, X_test, y_train, y_test = train_test_split(
        X, y,
        test_size=0.2,
        random_state=42,
        stratify=y,
    )
    logger.info(
        f"\n  Train: {len(X_train):,} строк | "
        f"Test: {len(X_test):,} строк"
    )

    # 3. Настраиваем MLflow
    # tracking_uri — где хранятся результаты (локально в папке mlruns/)
    mlflow.set_tracking_uri("sqlite:///mlflow.db")
    mlflow.set_experiment(EXPERIMENT_NAME)
    logger.info(f"  MLflow эксперимент: {EXPERIMENT_NAME}")

    # 4. Запуск 1: Базовая модель
    logger.info("\n  Запуск 1: Базовая логистическая регрессия...")
    metrics_1 = train_and_log(
        X_train, X_test, y_train, y_test,
        params={
            "model":        "LogisticRegression",
            "C":            1.0,
            "max_iter":     100,
            "class_weight": None,
        },
        run_name="baseline_C1.0",
    )

    # 5. Запуск 2: С балансировкой классов
    # class_weight='balanced' — важно для несбалансированных данных
    # У нас 3.5% фрода — модель без балансировки игнорирует фрод
    logger.info("\n  Запуск 2: С балансировкой классов...")
    metrics_2 = train_and_log(
        X_train, X_test, y_train, y_test,
        params={
            "model":        "LogisticRegression",
            "C":            1.0,
            "max_iter":     200,
            "class_weight": "balanced",
        },
        run_name="balanced_C1.0",
    )

    # 6. Запуск 3: Балансировка + другой C
    logger.info("\n  Запуск 3: Балансировка + C=0.1...")
    metrics_3 = train_and_log(
        X_train, X_test, y_train, y_test,
        params={
            "model":        "LogisticRegression",
            "C":            0.1,
            "max_iter":     200,
            "class_weight": "balanced",
        },
        run_name="balanced_C0.1",
    )

    # 7. Сравниваем результаты
    logger.info("\n" + "=" * 55)
    logger.info("  СРАВНЕНИЕ ЭКСПЕРИМЕНТОВ")
    logger.info("=" * 55)
    logger.info(f"  {'Метрика':<12} {'baseline':>10} {'balanced':>10} {'C=0.1':>10}")
    logger.info(f"  {'─' * 45}")
    for metric in ["accuracy", "precision", "recall", "f1", "roc_auc"]:
        logger.info(
            f"  {metric:<12} "
            f"{metrics_1[metric]:>10.4f} "
            f"{metrics_2[metric]:>10.4f} "
            f"{metrics_3[metric]:>10.4f}"
        )
    logger.info("=" * 55)
    logger.info("  Откройте MLflow UI:")
    logger.info("  mlflow ui → http://localhost:5000")
    logger.info("=" * 55)


if __name__ == "__main__":
    main()