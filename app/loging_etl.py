import logging
from colorama import Fore, Style, init

# Инициализация colorama для Windows
init()


class ColoredFormatter(logging.Formatter):
    """Форматтер с цветами для консоли"""

    COLORS = {
        logging.DEBUG: Fore.BLUE,
        logging.INFO: Fore.GREEN,
        logging.WARNING: Fore.YELLOW,
        logging.ERROR: Fore.RED,
        logging.CRITICAL: Fore.RED + Style.BRIGHT
    }

    def format(self, record):
        # Сохраняем оригинальные значения
        original_levelname = record.levelname
        original_msg = record.msg

        # Добавляем цвета только для отображения, не изменяя сами данные
        if record.levelno in self.COLORS:
            color = self.COLORS[record.levelno]
            record.levelname = f"{color}{record.levelname}{Style.RESET_ALL}"
            record.msg = f"{color}{record.msg}{Style.RESET_ALL}"

        result = super().format(record)

        # Восстанавливаем оригинальные значения
        record.levelname = original_levelname
        record.msg = original_msg

        return result


class FileFormatter(logging.Formatter):
    """Простой форматтер для файла без цветов"""

    def __init__(self):
        super().__init__(
            fmt="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
            datefmt="%Y-%m-%d %H:%M:%S"
        )


class SensitiveDataFilter(logging.Filter):
    """Фильтр для скрытия чувствительных данных"""

    def __init__(self):
        super().__init__()
        self.sensitive_keywords = ['password', 'token', 'secret', 'key', 'auth']

    def filter(self, record):
        message = record.getMessage().lower()
        return not any(keyword in message for keyword in self.sensitive_keywords)


def setup_logger():
    """Настройка логгера с разными обработчиками"""

    # Создаем логгер
    logger = logging.getLogger(__name__)
    logger.setLevel(logging.DEBUG)  # Устанавливаем максимальный уровень

    # Удаляем существующие обработчики, если есть
    logger.handlers.clear()

    # Создаем обработчики
    console_handler = logging.StreamHandler()
    file_handler = logging.FileHandler("etl.log", encoding='utf-8')

    # Устанавливаем уровни для обработчиков
    console_handler.setLevel(logging.INFO)  # В консоль только INFO и выше
    file_handler.setLevel(logging.DEBUG)  # В файл все сообщения

    # Создаем форматтеры
    console_formatter = ColoredFormatter(
        fmt="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S"
    )

    file_formatter = FileFormatter()

    # Применяем форматтеры
    console_handler.setFormatter(console_formatter)
    file_handler.setFormatter(file_formatter)

    # Добавляем фильтр к обоим обработчикам
    sensitive_filter = SensitiveDataFilter()
    console_handler.addFilter(sensitive_filter)
    file_handler.addFilter(sensitive_filter)

    # Добавляем обработчики к логгеру
    logger.addHandler(console_handler)
    logger.addHandler(file_handler)

    return logger


# Создаем и настраиваем логгер
logger = setup_logger()

# Пример использования
if __name__ == "__main__":
    logger.debug("Это debug сообщение - должно быть только в файле")
    logger.info("Это info сообщение")
    logger.warning("Это warning сообщение")
    logger.error("Это error сообщение")
    logger.critical("Это critical сообщение")

    # Сообщения, которые должны быть отфильтрованы
    logger.info("Пользователь ввел password: 12345")
    logger.info("Токен доступа: abcdefg")
    logger.info("Секретный ключ: xyz")

    logger.info("Нормальное сообщение без чувствительных данных")

# Запрет импорта через `from module import *`
__all__ = []