<h1 align="center"> Создание витрин данных для аналитиков

## Цель моей работы:      
#### Создать витрину данных для аналитиков, которая будет содержать агрегированные метрики по карточкам товаром, популярным категориям и прочим.       
## Состав проекта:        
- реализация задачи на Spark, и один DAG Airflow       
- создание запросов о построении Greenplum - витрины, исполняемого из DAG Airflow

## Необходимые технологии:
- Python (библиотеки для работы с данными, такие как Pandas, PySpark)       
- SQL (для работы с базами данных)       
- ETL-инструменты (например, Apache Airflow)       
- Облачные решения (AWS, GCP или аналогичные)       
- Знание принципов работы с большими данными (Big Data)

![изображение_2025-02-06_175055981](https://github.com/user-attachments/assets/a8fa62c4-f6a8-4cbe-bdc9-ef9d1571ad8b) 


## Задачи проекта: 
**Задача 1**.  
* **Необходимо создать pipeline обработки данных (DAG Airflow), который реализует все запросы из последующих шагов.**.  
* **Требования к Airflow DAG:**
1. DAG не имеет расписания (schedule_interval=None)      
2. Каждая Spark задача выполняется в своей таске (отдельный submit)       
3. Все таски выполняются параллельно     
**Задача 2. Обработка данных для отчета по LineItems**
* **Необходимо обогатить исходные данные дополнительными параметрами и агрегатами на основе показателей.**
  ![1750671768933](https://github.com/user-attachments/assets/e62e0fcc-fb57-4fcf-b8b8-0a6192a7d63c)
**Задача 3.Пайплайны витрин DWH**
* **Все таблицы должны создаваться в вашей схеме**
* **Создание таблиц должно происходить с помощью оператора SQLExecuteQueryOperator**
* **Каждый оператор должен вызываться последовательно после построения отчета и сохранения его в вашем S3.**
* **Описание требуемых структур таблиц представлено в последующих шагах.**
* **Создание таблиц должно быть идемпотентным.**
**Задача 4. Витрины**
  * **Для витрин нужно использовать Greenplum PXF**
  * **Витрина SellerItems.**
* Добавьте шаг items_datamart в DAG Airflow для создания внешней таблицы над вашими данными размещенными в S3.
* Таблица должна называться seller_items и размещена в вашей схеме.
**Создание View**
  * **View "Не надежные продавцы"**
  Добавьте шаг create_unreliable_sellers_report_view в DAG Airflow для создания view таблицы с перечнем ненадежных продавцов:
1. Если товар выставлен на торговую площадку более 100 дней. 
2. Количество товаров размещенных на складе больше, чем количество заказанных товаров.
3. В расчете учитываются все позиции предоставленные продавцов на площадке.
 * **View "Отчет брендов"**      
 Добавьте шаг create_brands_report_view в DAG Airflow для создания view таблицы с отчетом по брендам.      
![1750672537924](https://github.com/user-attachments/assets/0b2ac442-120a-4cb9-a0e0-0a325bcc3341)

## Этапы выполнения: 
#### 1.⁠ ⁠Импорт оператора для выполнения SQL-запросов:      
```from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator```         
#### 2.⁠ ⁠SQL-запросы для создания таблиц и view:        
````
``` items_datamart_sql = """DROP EXTERNAL TABLE IF EXISTS "anastasija-pulatova-fqe3933".seller_items CASCADE;
CREATE EXTERNAL TABLE "anastasija-pulatova-fqe3933".seller_items(
    sku_id BIGINT,
    title TEXT,
    category TEXT,
    brand TEXT,
    seller TEXT,
    group_type TEXT,
    country TEXT,
    availability_items_count BIGINT,
    ordered_items_count BIGINT,
    warehouses_count BIGINT,
    item_price BIGINT,
    goods_sold_count BIGINT,
    item_rate FLOAT8,
    days_on_sell BIGINT,
    avg_percent_to_sold BIGINT,
    returned_items_count INTEGER,
    potential_revenue BIGINT,
    total_revenue BIGINT,
    avg_daily_sales FLOAT8,
    days_to_sold FLOAT8,
    item_rate_percent FLOAT8
) LOCATION ('pxf://startde-project/anastasija-pulatova-fqe3933/seller_items?PROFILE=s3:parquet&SERVER=default')
ON ALL FORMAT 'CUSTOM' (FORMATTER='pxfwritable_import') ENCODING 'UTF8';"""

unreliable_sellers_view_sql = """CREATE OR REPLACE VIEW "anastasija-pulatova-fqe3933".unreliable_sellers_view AS 
    SELECT seller, 
           SUM(availability_items_count) as total_overload_items_count,  
           BOOL_OR(
                availability_items_count > ordered_items_count 
                AND days_on_sell > 100
            ) AS is_unreliable
    FROM "anastasija-pulatova-fqe3933".seller_items
    GROUP BY seller;"""

brands_report_view_sql = """CREATE OR REPLACE VIEW "anastasija-pulatova-fqe3933".item_brands_view AS 
    SELECT brand,
           group_type,
           country, 
           SUM(potential_revenue)::FLOAT8 as potential_revenue,  
           SUM(total_revenue)::FLOAT8 as total_revenue,
           COUNT(sku_id)::BIGINT as items_count
    FROM "anastasija-pulatova-fqe3933".seller_items
    GROUP BY brand, group_type, country;"""   
```
````

#### 3.Код для создания задач с использованием SQLExecuteQueryOperator соответствует заданию:              

- ⁠Создание таблицы seller_items:        

````
```
create_table_items_datamart_task = SQLExecuteQueryOperator(
    task_id="items_datamart",
    conn_id="greenplume_karpov",
    sql=items_datamart_sql,
    split_statements=True,
    return_last=False,
)
```
````     

- ⁠Создание представления unreliable_sellers_view:        
````
```
create_unreliable_sellers_report_view = SQLExecuteQueryOperator(
    task_id="create_unreliable_sellers_report_view",
    conn_id="greenplume_karpov",
    sql=unreliable_sellers_view_sql,
    split_statements=True,
    return_last=False,
)
```
````       


- ⁠Создание представления item_brands_view:        
````
```
create_brands_report_view = SQLExecuteQueryOperator(
    task_id="create_brands_report_view",
    conn_id="greenplume_karpov",
    sql=brands_report_view_sql,
    split_statements=True,
    return_last=False,
)
```
````
#### 4. Создание Dag
````
```
dag_id="startde-project-anastasija-pulatova-fqe3933-dag",
    default_args = default_args,
    schedule_interval=None,
    start_date=pendulum.datetime(2025, 5, 29, tz="UTC"),
    tags=["final_project"],
    catchup=False
) as dag:

    start = EmptyOperator(task_id="start", dag=dag)
    end = EmptyOperator(task_id="end", dag=dag)

    submit_task = _build_submit_operator(
        task_id=SUBMIT_NAME,
        application_file='spark_submit.yaml',
        link_dag=dag
    )

    sensor_task = _build_sensor(
        task_id='job_sensor',
        application_name=f"{{{{task_instance.xcom_pull(task_ids='{SUBMIT_NAME}')['metadata']['name']}}}}",
        link_dag=dag
    )

    create_table_items_datamart_task = SQLExecuteQueryOperator(
    task_id="items_datamart",
    conn_id = "greenplume_karpov",
    sql=items_datamart_sql,
    split_statements=True,
    return_last=False,
)
    
    create_unreliable_sellers_report_view = SQLExecuteQueryOperator(
    task_id="create_unreliable_sellers_report_view",
    conn_id = "greenplume_karpov",
    sql=unreliable_sellers_view_sql,
    split_statements=True,
    return_last=False,
)
    
    create_brands_report_view = SQLExecuteQueryOperator(
    task_id="create_brands_report_view",
    conn_id = "greenplume_karpov",
    sql=brands_report_view_sql,
    split_statements=True,
    return_last=False,
)

    start >> submit_task >> sensor_task >> create_table_items_datamart_task >> [create_unreliable_sellers_report_view, create_brands_report_view] >> end
```
````

## Полученный результат: 
- Реализовала автоматизированные процессы обработки данных в Apache Airflow с использованием SQLExecuteQueryOperator для выполнения запросов к базе данных Greenplum.
- Создала внешние таблицы и представления, включая "seller_items", "unreliable_sellers_view" и "item_brands_view", что улучшило анализ данных о товарах и продавцах.
- Оптимизировала процесс получения аналитической информации, что способствовало более эффективной отчетности и принятия решений на основе данных.
