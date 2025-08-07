# 🚂 RZhD Scripts - Система обработки данных Российских железных дорог

## 📋 Описание проекта

Данный репозиторий содержит систему автоматизированной обработки данных железнодорожных перевозок РЖД. Система предназначена для конвертации Excel-файлов с отчетами о перевозках в структурированные JSON-данные с последующей загрузкой в аналитическое хранилище.

## ⚙️ Основная функциональность

### 🔧 Блоки работ

1. **📦 Обработка данных КТК** (`rzhd_ktk.py`) - Специализированная обработка контейнерных перевозок
2. **📅 Еженедельная обработка** (`rzhd_weekly.py`) - Агрегация еженедельных отчетов
3. **🌍 Обработка по регионам**:
   - 🏭 Новороссийск (`rzhd_novorossiysk.sh`)
   - 🏛️ Санкт-Петербург (`rzhd_petersburg.sh`)
4. **⚡ Базовая обработка** (`rzhd.py`) - Основная логика конвертации данных

### 🚀 Возможности системы

- ✅ Автоматический мониторинг папок с входящими файлами
- ✅ Поддержка множественных форматов Excel (.xls, .xlsx, .xlsb)
- ✅ Валидация и нормализация данных
- ✅ Обработка ошибок с логированием
- ✅ Интеграция с ClickHouse для хранения данных
- ✅ Система уведомлений через Telegram
- ✅ Контейнеризация через Docker

## 🏗️ Архитектура системы

```
rzhd_scripts/
├── scripts/                    # Python модули
│   ├── rzhd.py                # Базовый класс обработки
│   ├── rzhd_ktk.py            # Обработка КТК данных
│   ├── rzhd_weekly.py         # Еженедельная обработка
│   ├── headers.py             # Mapping заголовков
│   ├── app_logger.py          # Модуль логирования
│   └── __init__.py            # Конфигурация и константы
├── bash/                      # Bash скрипты запуска
│   ├── run_rzhd.sh           # Главный скрипт запуска
│   ├── rzhd_ktk.sh           # Обработка КТК
│   ├── rzhd_novorossiysk.sh  # Обработка Новороссийск
│   ├── rzhd_petersburg.sh    # Обработка СПб
│   └── rzhd_weekly.sh        # Еженедельная обработка
├── Dockerfile                 # Конфигурация контейнера
├── docker-compose.yml         # Docker Compose конфигурация
├── requirements.txt           # Python зависимости
├── .env                       # Переменные окружения
└── README.md                 # Данная документация
```

## 💻 Технический стек

### 🛠️ Основные технологии
- **🐍 Python 3.8+** - Основной язык разработки
- **🐳 Docker** - Контейнеризация приложения
- **📝 Bash** - Скрипты оркестрации
- **🗄️ ClickHouse** - Аналитическая база данных

### 📚 Python библиотеки
```
pandas==1.4.3          # Обработка данных
numpy==1.23.3           # Численные вычисления  
openpyxl==3.1.1         # Работа с .xlsx файлами
pyxlsb==1.0.10          # Работа с .xlsb файлами
xlrd==2.0.1             # Работа с .xls файлами
clickhouse-connect==0.5.14  # Коннектор к ClickHouse
requests~=2.31.0        # HTTP запросы
notifiers==1.3.3        # Уведомления
python-dotenv==1.0.0    # Управление переменными окружения
```

## 🔧 Установка и настройка

### ✅ Предварительные требования

1. **🐳 Docker** и **Docker Compose**
2. **🐍 Python 3.8+** (для локальной разработки)
3. **🗄️ ClickHouse** сервер (для хранения данных)

### 🌍 Переменные окружения

Скопируйте файл `.env.example` в `.env` и заполните своими значениями:

```bash
cp .env.example .env
```

Или создайте файл `.env` в корне проекта вручную:

```bash
# Пути к данным
XL_IDP_ROOT_RZHD=/path/to/rzhd/scripts
XL_IDP_PATH_RZHD=/path/to/input/data
XL_IDP_PATH_DOCKER=/path/to/docker/data
XL_IDP_PATH_RZHD_SCRIPTS=/path/to/rzhd/scripts

# ClickHouse настройки
CLICKHOUSE_HOST=your_host
CLICKHOUSE_PORT=your_port
CLICKHOUSE_DATABASE=your_database
CLICKHOUSE_USER=your_user
CLICKHOUSE_PASSWORD=your_password

# Telegram уведомления
TELEGRAM_BOT_TOKEN=your_bot_token
TOKEN_TELEGRAM=your_bot_token
TELEGRAM_CHAT_ID=your_chat_id
```

### 🚀 Сборка и запуск

#### 🐳 Docker Compose (рекомендуемый способ)

1. **🔨 Сборка и запуск с Docker Compose:**

```yaml
version: "3.9"
services:
    rzhd:
        container_name: rzhd
        restart: always
        ports:
        - "8100:8100"
        volumes:
        - ${XL_IDP_PATH_RZHD_SCRIPTS}:${XL_IDP_PATH_DOCKER}
        - ${XL_IDP_ROOT_RZHD}:${XL_IDP_PATH_RZHD}
        environment:
        XL_IDP_ROOT_RZHD: ${XL_IDP_PATH_DOCKER}
        XL_IDP_PATH_RZHD: ${XL_IDP_PATH_RZHD}
        TOKEN_TELEGRAM: ${TOKEN_TELEGRAM}
        build:
        context: rzhd
        dockerfile: ./Dockerfile
        args:
            XL_IDP_PATH_DOCKER: ${XL_IDP_PATH_DOCKER}
        command:
        bash -c "sh ${XL_IDP_PATH_DOCKER}/bash/run_rzhd.sh"
        networks:
        - postgres
```

```bash
docker-compose up -d --build
```

2. **📄 Просмотр логов:**
```bash
docker-compose logs -f rzhd
```

3. **⏹️ Остановка сервиса:**
```bash
docker-compose down
```

#### 🐳 Docker (альтернативный способ)

1. **🔨 Сборка образа:**
```bash
docker build -t rzhd-scripts .
```

2. **▶️ Запуск контейнера:**
```bash
docker run -d \
  --name rzhd-processor \
  -v /path/to/data:/data \
  -v /path/to/logs:/app/logging \
  --env-file .env \
  rzhd-scripts
```

#### 💻 Локальная установка

1. **🏗️ Создание виртуального окружения:**
```bash
python3 -m venv venv
source venv/bin/activate  # Linux/Mac
# или
venv\Scripts\activate     # Windows
```

2. **📦 Установка зависимостей:**
```bash
pip install -r requirements.txt
```

3. **🚀 Запуск обработки:**
```bash
# Запуск всех модулей
bash/run_rzhd.sh

# Запуск отдельных модулей
bash/rzhd_ktk.sh
bash/rzhd_weekly.sh
```

### 📋 Конфигурация Docker Compose

Файл `docker-compose.yml` содержит полную конфигурацию для развертывания системы:

- **🔗 Сеть**: Подключение к внешней сети `postgres` для взаимодействия с базой данных
- **📂 Volumes**: Автоматическое монтирование директорий со скриптами и данными
- **🔄 Restart Policy**: Автоматический перезапуск контейнера при сбоях
- **🚪 Порты**: Экспорт порта 8100 для мониторинга (при необходимости)
- **🌍 Environment**: Передача переменных окружения из `.env` файла

## 📖 Использование

### 📁 Структура входных данных

Система ожидает Excel файлы в следующих директориях:
```
${XL_IDP_PATH_RZHD}/
├── rzhd_ktk/           # КТК данные
├── rzhd_novorossiysk/  # Данные Новороссийска  
├── rzhd_petersburg/    # Данные СПб
└── rzhd_weekly/        # Еженедельные отчеты
```

### 🔄 Процесс обработки

1. **👀 Мониторинг файлов** - Система автоматически отслеживает новые .xls/.xlsx/.xlsb файлы
2. **✅ Валидация** - Проверка структуры и содержимого файлов
3. **🔄 Конвертация** - Преобразование данных в JSON с нормализацией
4. **📤 Загрузка** - Отправка данных в ClickHouse
5. **📦 Архивирование** - Перемещение обработанных файлов в папку `done/`
6. **📢 Уведомления** - Отправка статуса обработки в Telegram

### 📄 Формат выходных данных

Обработанные данные сохраняются в JSON формате со стандартизованными полями:
```json
{
  "departure_date": "2024-01-15",
  "arrival_date": "2024-01-20", 
  "cargo_name": "Контейнер 20 футов",
  "shipper_okpo": "12345678",
  "consignee_okpo": "87654321",
  "transportation_volume_tons": 25.5,
  "wagon_number": "12345678",
  "container_no": "ABCD1234567"
}
```

## 📊 Мониторинг и логирование

### 📝 Логи
Логи сохраняются в директории `${XL_IDP_ROOT_RZHD}/logging/`:
- 📄 `rzhd.log` - Основные операции
- 📦 `rzhd_ktk.log` - КТК обработка
- 📅 `rzhd_weekly.log` - Еженедельная обработка

### 🚨 Мониторинг ошибок
- ❌ Ошибочные файлы переименовываются с префиксом `error_`
- 📢 Уведомления о критических ошибках отправляются в Telegram
- 📋 Детальное логирование всех операций

## 👨‍💻 Разработка

### 🏗️ Структура классов

#### 📚 Базовый класс `Rzhd`
```python
class Rzhd:
    def __init__(self, filename: str, folder: str)
    def convert_to_float(value: str) -> Union[float, None]
    def convert_to_int(value: str) -> Union[int, None]
    def divide_chunks(list_data: list, chunk: int) -> Generator
```

#### 🔧 Специализированные классы
- 📦 `RzhdKTK(Rzhd)` - Обработка контейнерных данных
- 📅 `RzhdWeekly(Rzhd)` - Еженедельная агрегация

### ➕ Добавление нового типа обработки

1. **📝 Создайте новый Python модуль** в `scripts/`
2. **🔗 Наследуйтесь от базового класса** `Rzhd`
3. **📝 Добавьте bash скрипт** в `bash/`
4. **🔄 Обновите главный скрипт** `run_rzhd.sh`

### 🧪 Тестирование

```bash
# Запуск отдельного модуля для тестирования
python3 scripts/rzhd_ktk.py /path/to/test/file.xlsx /path/to/output/

# Проверка логов
tail -f logging/rzhd_ktk.log
```