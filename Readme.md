## 요구사항
1. KST 기준 daily partition 처리
2. 동일```user_id```내에서 ```event_time```간격이 5분 이상인 경우 세션 종료로 간주하고 새로운 세션 ID를 생성하세요
3. 재처리 후 parquet, snappy 처리
4. External Table 방식으로 설계 하고, 추가 기간 처리에 대응가능하도록 구현
5. 배치 장애시 복구를 위한 장치 구현
6. 설계한 Hive external 테이블을 이용하여 WAU(Weekly Active Users) 계산해주세요
    1. user_id 를 기준으로 WAU를 계산하세요
    2. 2에서 생성된 세션 ID를 기준으로 WAU를 계산하세요
    3. 6-a, 6-b의 결과 값과 계산에 사용한 쿼리도 함께 전달해주세요

### 파일별 Schema 정리
[2019-Oct.csv 스키마] :
```
root
|-- event_time: timestamp (nullable = true)
|-- event_type: string (nullable = true)
|-- product_id: integer (nullable = true)
|-- category_id: long (nullable = true)
|-- category_code: string (nullable = true)
|-- brand: string (nullable = true)
|-- price: double (nullable = true)
|-- user_id: integer (nullable = true)
|-- user_session: string (nullable = true)
```
### 1. KST 기준 daily partition 처리
## 요구사항
1. KST 기준 daily partition 처리
2. 동일```user_id```내에서 ```event_time```간격이 5분 이상인 경우 세션 종료로 간주하고 새로운 세션 ID를 생성하세요
3. 재처리 후 parquet, snappy 처리
4. External Table 방식으로 설계 하고, 추가 기간 처리에 대응가능하도록 구현
5. 배치 장애시 복구를 위한 장치 구현
6. 설계한 Hive external 테이블을 이용하여 WAU(Weekly Active Users) 계산해주세요
   1. user_id 를 기준으로 WAU를 계산하세요
   2. 2에서 생성된 세션 ID를 기준으로 WAU를 계산하세요
   3. 6-a, 6-b의 결과 값과 계산에 사용한 쿼리도 함께 전달해주세요

### 파일별 Schema 정리
[2019-Oct.csv 스키마] :
```
root
|-- event_time: timestamp (nullable = true)
|-- event_type: string (nullable = true)
|-- product_id: integer (nullable = true)
|-- category_id: long (nullable = true)
|-- category_code: string (nullable = true)
|-- brand: string (nullable = true)
|-- price: double (nullable = true)
|-- user_id: integer (nullable = true)
|-- user_session: string (nullable = true)
```

[2019-Nov.csv 스키마] :
```
root
|-- event_time: timestamp (nullable = true)
|-- event_type: string (nullable = true)
|-- product_id: integer (nullable = true)
|-- category_id: long (nullable = true)
|-- category_code: string (nullable = true)
|-- brand: string (nullable = true)
|-- price: double (nullable = true)
|-- user_id: integer (nullable = true)
|-- user_session: string (nullable = true)
```

[External Table Schema] :
```sql
event_time TIMESTAMP,
event_type STRING,
product_id INT,
category_id BIGINT,
category_code STRING,
brand STRING,
price DOUBLE,
user_id INT,
user_session STRING,
event_time_kst TIMESTAMP,
session_id STRING
```
## 정리
### 1. KST 기준 daily partition 처리
- Spark SQL의 `from_utc_timestamp`를 사용하여 `event_time`을 `"Asia/Seoul"`(KST) 기준으로 변환 후 `yyyyMMdd` 포맷으로 `partition_date` 컬럼 생성.

### 2. 5분 기준 session id 생성
- 각 ```user_id```별 이벤트를 시간순으로 정렬 후, ```lag()```와 ```unix_timestamp()```로 이전 이벤트와의 시간 차이를 계산 후 MD5 해싱으로 session_id 생성

### 3. Snappy 압축 + Parquet 저장
- 세션 처리된 결과를 ```.parquet``` 포맷으로 저장하며 Snappy 압축을 적용
- 저장 경로는 ```partition_date```를 기준으로 파티셔닝

### 4. Hive External Table 생성 및 유지
- 저장된 Parquet 경로를 기반으로 Hive External Table 생성
- ```partition_date```를 파티션 컬럼으로 지정
- ```MSK REPAIR TABLE```를 통해 신규 파티션 반영

### 5. 배치 장애 복구
- 이미 저장된 ```partition_date``` 디렉토리를 확인해 중복 저장 방지 및 없는 날짜만 필터링 해 저장

### 6-1.
[user_id 기준 WAU 계산 쿼리]
```sql
SELECT
  weekofyear(event_time_kst) AS week_number,
  MIN(DATE(event_time_kst)) AS start_date,
  MAX(DATE(event_time_kst)) AS end_date,
  COUNT(DISTINCT user_id) AS wau_user_id
FROM backpackr_events
GROUP BY weekofyear(event_time_kst)
ORDER BY week_number
```
[6-1. 결과값]
```
+-----------+----------+----------+-----------+
|week_number|start_date|end_date  |wau_user_id|
+-----------+----------+----------+-----------+
|40         |2019-10-01|2019-10-06|756699     |
|41         |2019-10-07|2019-10-13|1049906    |
|42         |2019-10-14|2019-10-20|1097548    |
|43         |2019-10-21|2019-10-27|1102226    |
|44         |2019-10-28|2019-11-03|1039579    |
|45         |2019-11-04|2019-11-10|1307550    |
|46         |2019-11-11|2019-11-17|1493197    |
|47         |2019-11-18|2019-11-24|1475270    |
|48         |2019-11-25|2019-12-01|1248791    |
+-----------+----------+----------+-----------+
```

### 6-2.
[session_id 기준 WAU 계산 쿼리]
```sql
SELECT
  weekofyear(event_time_kst) AS week_number,
  MIN(DATE(event_time_kst)) AS start_date,
  MAX(DATE(event_time_kst)) AS end_date,
  COUNT(DISTINCT session_id) AS wau_session_id
FROM backpackr_events
GROUP BY weekofyear(event_time_kst)
ORDER BY week_number
```
[6-2. 결과값]
```
+-----------+----------+----------+--------------+
|week_number|start_date|end_date  |wau_session_id|
+-----------+----------+----------+--------------+
|40         |2019-10-01|2019-10-06|1421372       |
|41         |2019-10-07|2019-10-13|2109281       |
|42         |2019-10-14|2019-10-20|2281765       |
|43         |2019-10-21|2019-10-27|2170337       |
|44         |2019-10-28|2019-11-03|2079836       |
|45         |2019-11-04|2019-11-10|2719770       |
|46         |2019-11-11|2019-11-17|4323631       |
|47         |2019-11-18|2019-11-24|3336741       |
|48         |2019-11-25|2019-12-01|2564452       |
+-----------+----------+----------+--------------+
```

[2019-Nov.csv 스키마] :
```
root
|-- event_time: timestamp (nullable = true)
|-- event_type: string (nullable = true)
|-- product_id: integer (nullable = true)
|-- category_id: long (nullable = true)
|-- category_code: string (nullable = true)
|-- brand: string (nullable = true)
|-- price: double (nullable = true)
|-- user_id: integer (nullable = true)
|-- user_session: string (nullable = true)
```

[External Table Schema] :
```sql
event_time TIMESTAMP,
event_type STRING,
product_id INT,
category_id BIGINT,
category_code STRING,
brand STRING,
price DOUBLE,
user_id INT,
user_session STRING,
event_time_kst TIMESTAMP,
session_id STRING
```

### 6-1.
[user_id 기준 WAU 계산 쿼리]
```sql
SELECT
  weekofyear(event_time_kst) AS week_number,
  MIN(DATE(event_time_kst)) AS start_date,
  MAX(DATE(event_time_kst)) AS end_date,
  COUNT(DISTINCT user_id) AS wau_user_id
FROM backpackr_events
GROUP BY weekofyear(event_time_kst)
ORDER BY week_number
```
[6-1. 결과값]
```
+-----------+----------+----------+-----------+
|week_number|start_date|end_date  |wau_user_id|
+-----------+----------+----------+-----------+
|40         |2019-10-01|2019-10-06|756699     |
|41         |2019-10-07|2019-10-13|1049906    |
|42         |2019-10-14|2019-10-20|1097548    |
|43         |2019-10-21|2019-10-27|1102226    |
|44         |2019-10-28|2019-11-03|1039579    |
|45         |2019-11-04|2019-11-10|1307550    |
|46         |2019-11-11|2019-11-17|1493197    |
|47         |2019-11-18|2019-11-24|1475270    |
|48         |2019-11-25|2019-12-01|1248791    |
+-----------+----------+----------+-----------+
```

### 6-2.
[session_id 기준 WAU 계산 쿼리]
```sql
SELECT
  weekofyear(event_time_kst) AS week_number,
  MIN(DATE(event_time_kst)) AS start_date,
  MAX(DATE(event_time_kst)) AS end_date,
  COUNT(DISTINCT session_id) AS wau_session_id
FROM backpackr_events
GROUP BY weekofyear(event_time_kst)
ORDER BY week_number
```
[6-2. 결과값]
```
+-----------+----------+----------+--------------+
|week_number|start_date|end_date  |wau_session_id|
+-----------+----------+----------+--------------+
|40         |2019-10-01|2019-10-06|1421372       |
|41         |2019-10-07|2019-10-13|2109281       |
|42         |2019-10-14|2019-10-20|2281765       |
|43         |2019-10-21|2019-10-27|2170337       |
|44         |2019-10-28|2019-11-03|2079836       |
|45         |2019-11-04|2019-11-10|2719770       |
|46         |2019-11-11|2019-11-17|4323631       |
|47         |2019-11-18|2019-11-24|3336741       |
|48         |2019-11-25|2019-12-01|2564452       |
+-----------+----------+----------+--------------+
```

### Scala 선택 이유
> DataFrame, Spark SQL 기능들이 Java보다 Scala에서 더 직관적이라고 판단했습니다.
1. Java의 경우 ```$"컬럼명"```과 같은 DSL이 없어서 ```col("...")```을 계속 사용해야 하는 번거로움이 있습니다.
2. Java의 경우 multi-line string을 지원하지 않아서 SQL 쿼리를 작성할 때 불편함이 있습니다. 반면 Scala의 경우 stripMargin을 지원합니다.
3. 이외에도 UDF 작성이나 FP 스타일을 지원하는 Scala가 더 직관적이라고 판단했습니다.