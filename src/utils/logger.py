"""
Настройка логирования
=====================
Используем loguru — современная замена стандартному logging.
Один раз настраиваем здесь, импортируем logger везде.
"""

import sys
from loguru import logger


def setup_logger(level: str = "INFO") -> None:
    """
    Настраивает формат и уровень логирования.
    Вызывается один раз при старте пайплайна.
    """
    # Убираем дефолтный handler
    logger.remove()

    # Добавляем красивый вывод в консоль
    logger.add(
        sys.stdout,
        level=level,
        format=(
            "<green>{time:YYYY-MM-DD HH:mm:ss}</green> | "
            "<level>{level: <8}</level> | "
            "<cyan>{name}</cyan>:<cyan>{function}</cyan> | "
            "<level>{message}</level>"
        ),
        colorize=True,
    )


# Настраиваем при импорте
setup_logger()

# Экспортируем logger для использования в других модулях
# Использование: from src.utils.logger import logger
__all__ = ["logger", "setup_logger"]