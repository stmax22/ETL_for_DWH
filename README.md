## Описание проекта
Проект реализует ETL-пайплайн для загрузки данных из внешнего API в хранилище данных (DWH) с построением аналитических витрин. Система поддерживает инкрементальную загрузку, контроль качества данных (Data Quality) и автоматизацию через Apache Airflow.

## Архитектура решения
Проект реализует полный цикл ETL-процесса:
1. **Extract** — извлечение данных через REST API и S3 хранилища
2. **Transform** — трансформация данных с помощью Python (Pandas)
3. **Load** — загрузка в Staging-слой PostgreSQL
4. **Datamart** — обновление таблиц измерений (SCD1) и фактов
5. **Quality Control** — автоматические проверки качества данных

## Структура проекта
```
.
├── ETL_DAG.py                                # Основной DAG
├── Script.py                                 # Python-скрипты
└── sql/                                      # SQL-скрипты
    ├── create_schema_stage.sql               # Создание Stage-слоя
    ├── create_schema_mart.sql                # Создание DataMarts-слоя
    ├── create_schema_checks.sql              # Создание схемы для проверок
    ├── mart.d_calendar.sql                   # Заполнение календаря
    ├── mart.d_city.sql                       # Заполнение справочника городов
    ├── mart.d_customer.sql                   # Заполнение справочника клиентов
    ├── mart.d_item.sql                       # Заполнение справочника товаров
    ├── mart.f_sales.sql                      # Заполнение фактов продаж
    ├── mart.f_customer_retention.sql         # Витрина удержания клиентов
    ├── check_user_order_log_isNull.sql       # Проверка NULL в customer_id (user_order_log)
    ├── check_user_activity_log_isNull.sql    # Проверка NULL в customer_id (user_activity_log)
    ├── check_row_count_user_order_log.sql    # Проверка минимального количества записей (user_order_log)
    └── check_row_count_user_activity_log.sql # Проверка минимального количества записей (user_activity_log)
```

## Источники данных
Данные поступают из внешнего API, который предоставляет следующие файлы:
- `customer_research_inc.csv` — инкрементальные данные исследований
- `user_order_log_inc.csv` — инкрементальные данные заказов
- `user_activity_log_inc.csv` — инкрементальные данные активности

## Схемы данных

### Stage-слой
Слой для загрузки сырых данных из источников.

#### Таблица customer_research
| Поле | Тип | Описание |
|------|-----|----------|
| `id` | `INTEGER` | Идентификатор записи (PK) |
| `date_id` | `TIMESTAMP` | Дата записи |
| `category_id` | `INTEGER` | Идентификатор категории |
| `geo_id` | `INTEGER` | Идентификатор геоданных |
| `sales_qty` | `INTEGER` | Количество продаж |
| `sales_amt` | `NUMERIC(14,2)` | Сумма продаж |
| `status` | `VARCHAR(10)` | Статус заказа (shipped/refunded) |

#### Таблица user_order_log
| Поле | Тип | Описание |
|------|-----|----------|
| `id` | `INTEGER` | Идентификатор записи (PK) |
| `uniq_id` | `VARCHAR(32)` | Уникальный идентификатор |
| `date_time` | `TIMESTAMP` | Дата и время заказа |
| `city_id` | `INTEGER` | Идентификатор города |
| `city_name` | `VARCHAR(100)` | Название города |
| `customer_id` | `BIGINT` | Идентификатор клиента |
| `first_name` | `VARCHAR(100)` | Имя клиента |
| `last_name` | `VARCHAR(100)` | Фамилия клиента |
| `item_id` | `INTEGER` | Идентификатор товара |
| `item_name` | `VARCHAR(100)` | Название товара |
| `quantity` | `BIGINT` | Количество товара |
| `payment_amount` | `NUMERIC(14,2)` | Сумма платежа |
| `status` | `VARCHAR(10)` | Статус заказа (shipped/refunded) |

#### Таблица user_activity_log
| Поле | Тип | Описание |
|------|-----|----------|
| `id` | `INTEGER` | Идентификатор записи (PK) |
| `uniq_id` | `VARCHAR(32)` | Уникальный идентификатор |
| `date_time` | `TIMESTAMP` | Дата и время активности |
| `action_id` | `INTEGER` | Идентификатор действия |
| `customer_id` | `INTEGER` | Идентификатор клиента |
| `quantity` | `INTEGER` | Количество |
| `status` | `VARCHAR(10)` | Статус заказа (shipped/refunded) |

### DataMart-слой
Слой аналитических витрин в модели "Звезда".

#### Таблицы измерений
| Таблица | Описание |
|---------|----------|
| `d_calendar` | Календарь с датами, днями, месяцами, годами |
| `d_city` | Справочник городов |
| `d_customer` | Справочник клиентов |
| `d_item` | Справочник товаров |

#### Таблица фактов f_sales
| Поле | Тип | Описание |
|------|-----|----------|
| `date_id` | `TIMESTAMP` | Идентификатор даты |
| `item_id` | `INTEGER` | Идентификатор товара |
| `customer_id` | `INTEGER` | Идентификатор клиента |
| `city_id` | `INTEGER` | Идентификатор города |
| `quantity` | `INTEGER` | Количество (отрицательное для refunded) |
| `payment_amount` | `NUMERIC(14,2)` | Сумма платежа (отрицательная для refunded) |
| `order_status` | `VARCHAR(10)` | Статус заказа (shipped/refunded) |

#### Витрина f_customer_retention
| Поле | Тип | Описание |
|------|-----|----------|
| `new_customers_count` | `SMALLINT` | Количество новых клиентов |
| `returning_customers_count` | `SMALLINT` | Количество вернувшихся клиентов |
| `refunded_customer_count` | `SMALLINT` | Количество клиентов с возвратами |
| `period_name` | `VARCHAR(10)` | Название периода (weekly) |
| `period_id` | `SMALLINT` | Идентификатор периода (номер недели) |
| `item_id` | `INTEGER` | Идентификатор товара |
| `new_customers_revenue` | `SMALLINT` | Доход от новых клиентов |
| `returning_customers_revenue` | `SMALLINT` | Доход от вернувшихся клиентов |
| `customers_refunded` | `SMALLINT` | Количество возвратов |

### Схема checks (Data Quality)

#### Таблица dq_checks_results
| Поле | Тип | Описание |
|------|-----|----------|
| `table_name` | `VARCHAR(50)` | Название таблицы/файла |
| `check_name` | `VARCHAR(50)` | Название проверки |
| `check_date` | `TIMESTAMP` | Дата проверки |
| `check_status` | `NUMERIC(1)` | 0 — успех, 1 — ошибка |

## Контроль качества данных (Data Quality)
Проект включает 3 уровня автоматических проверок:

### Проверка №1 — Наличие файлов
- **Тип**: FileSensor
- **Проверяет**: Наличие всех трёх файлов
- **Файлы**: `customer_research_inc.csv`, `user_order_log_inc.csv`, `user_activity_log_inc.csv`
- **Действие при ошибке**: Остановка процесса

### Проверка №2 — NULL в customer_id
- **Тип**: SQLValueCheckOperator
- **Проверяет**: Отсутствие NULL в поле `customer_id`
- **Таблицы**: `user_order_log`, `user_activity_log`
- **Действие при ошибке**: Продолжение процесса с записью в лог

### Проверка №3 — Минимальное количество записей
- **Тип**: SQLCheckOperator
- **Проверяет**: Количество уникальных клиентов больше 3-х
- **Таблицы**: `user_order_log`, `user_activity_log`
- **Действие при ошибке**: Остановка процесса

## Поток данных (DAG)
```
generate_report → get_report → get_increment → create_local_files → 
check_files_name → load_files_to_bd → sql_check → 
update_data_mart_d → update_f_sales → update_f_customer_retention
```

### Описание этапов
1. **generate_report** — Инициализация формирования отчёта через API
2. **get_report** — Ожидание готовности отчёта (проверяем каждые 70 сек, max 4 попытки)
3. **get_increment** — Получение increment_id для загрузки данных за конкретную дату
4. **create_local_files** — Создание локальных CSV-файлов из S3
5. **check_files_name** — Проверка наличия файлов через FileSensor
6. **load_files_to_bd** — Загрузка данных в Stage-слой PostgreSQL
7. **sql_check** — Проверки качества данных (name, NULL, row count)
8. **update_data_mart_d** — Обновление таблиц измерений (SCD1)
9. **update_f_sales** — Обновление фактов продаж с учётом статуса refunded
10. **update_f_customer_retention** — Расчёт метрик удержания клиентов

## Особенности реализации

### Обработка статусов заказов
- **shipped** — стандартная продажа (положительные quantity и payment_amount)
- **refunded** — возврат (отрицательные quantity и payment_amount для корректного расчёта revenue)

### SCD1 для измерений
- Измерения обновляются полностью (DELETE + INSERT)
- История изменений не сохраняется
- Всегда актуальные данные

### Инкрементальная загрузка фактов
- DELETE данных за текущую дату из витрины
- INSERT новых данных из Stage
- Возможность перезапуска без дублирования

### f_customer_retention
Витрина `f_customer_retention` рассчитывает:
- **new** — клиенты с 1-м заказом за неделю
- **returning** — клиенты с больше 1-го заказа за неделю
- **refunded** — клиенты с возвратами
- Расчёт по неделям (weekly) с группировкой по товарам
