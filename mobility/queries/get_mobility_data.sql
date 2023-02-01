export to parquet(directory = 'hdfs://192.168.59.85:9000/apps/spark/mobilitydwh/fact_tb_staging')
over(partition by CELL_ID)
as
    select * from
    (
    select 
        START_TIME,
        cast(ftb.FIRST_CELL_FK as INT) as CELL_ID,
        HASH(mstb.MOBSUBS_ID) as HASHED_IMSI,
        drtb.DRT_NAME,
        MAX(DEVICE_FK) as DEVICE_FK,
        MAX(SUM_NO_OF_TDR) as SUM_NO_OF_TDR
    from 
        AINTDWH.AINT_HOUR_AGGR as ftb
    left join
        AINTDWH.DWH_AGGREGATION_TB as htb on ftb.HR_ID = htb.TIME_INTERVAL_ID
    left join
        AINTDWH.CELL_TB as ctb on ftb.FIRST_CELL_FK = ctb.CELL_ID
    left join
        AINTDWH.DATA_RECORD_TYPE_TB as drtb on ftb.DRT_FK = drtb.DRT_ID
    left join 
        AINTDWH.MOBILE_SUBSCRIBER_TB as mstb on ftb.IMSI_PR_FK = mstb.MOBSUBS_ID
    where ftb.FIRST_CELL_FK > 0
    group by cast(ftb.FIRST_CELL_FK as INT), START_TIME, CELL_ID, HASH(mstb.MOBSUBS_ID), DRT_NAME
    ) a 
limit 100
;
-- sudo -u dbadmin /opt/anritsu/Hadoop/hadoop-3.3.4/bin/hadoop fs -rm -r /apps/spark/mobilitydwh/records_test
--SELECT EXTERNAL_CONFIG_CHECK();

