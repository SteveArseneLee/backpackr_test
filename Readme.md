1. KST 기준 daily partition 처리
2. 동일```user_id```내에서 ```event_time```간격이 5분 이상인 경우 세션 종료로 간주하고 새로운 세션 ID를 생성하세요
3. 재처리 후 parquet, snappy 처리
4. External Table 방식으로 설계 하고, 추가 기간 처리에 대응가능하도록 구현
5. 배치 장애시 복구를 위한 장치 구현
6. 설계한 Hive external 테이블을 이용하여 WAU(Weekly Active Users) 계산해주세요
    1. user_id 를 기준으로 WAU를 계산하세요
    2. 2에서 생성된 세션 ID를 기준으로 WAU를 계산하세요
    3. 6-a, 6-b의 결과 값과 계산에 사용한 쿼리도 함께 전달해주세요