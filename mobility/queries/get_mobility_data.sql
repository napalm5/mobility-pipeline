export to parquet(directory = 'hdfs://192.168.59.85:9000/apps/spark/mobilitydwh/fact_tb_staging')
over(partition by USER_ID)
as
select
    *
from
(
with 
    call2g as (
        select 
            HASH(mstb.MOBSUBS_ID) as USER_ID,
            START_TIME_LOCAL,
            ctb.LATITUDE as LATITUDE,
            ctb.LONGITUDE as LONGITUDE,
            DRT_FK as TRANSACTION_TYPE,
            '2g CP' as TYPE
        from
            AINTDWH.AINT_TDR_FACT_TB as ftb
        left join 
            AINTDWH.CELL_TB as ctb on ftb.FIRST_CELL_FK = ctb.CELL_ID
        left join 
            AINTDWH.MOBILE_SUBSCRIBER_TB as mstb on ftb.IMSI_PR_FK = mstb.MOBSUBS_ID
        where 
            ctb.LATITUDE is not null and -- also longitude
            mstb.MOBSUBS_ID is not null and
            START_TIME_LOCAL is not null
    ),
    call3g as (
        select 
            HASH(mstb.MOBSUBS_ID) as USER_ID,
            START_TIME_LOCAL,
            ctb.LATITUDE as LATITUDE,
            ctb.LONGITUDE as LONGITUDE,
            DRT_FK as TRANSACTION_TYPE,
            '3g CP' as TYPE
        from
            IUCSDWH.IUCS_TDR_FACT_TB as ftb
        left join 
            IUCSDWH.CELL_TB as ctb on ftb.SC_FK = ctb.CELL_ID
        left join 
            IUCSDWH.MOBILE_SUBSCRIBER_TB as mstb on ftb.IMSI_FK = mstb.MOBSUBS_ID
        where 
            ctb.LATITUDE is not null and -- also longitude
            mstb.MOBSUBS_ID is not null and
            START_TIME_LOCAL is not null
    ),
    internet as (
        select 
            HASH(mstb.MOBSUBS_ID) as USER_ID,
            START_TIME_LOCAL,
            ctb.LATITUDE as LATITUDE,
            ctb.LONGITUDE as LONGITUDE,
            DRT_FK as TRANSACTION_TYPE,
            'UP - all' as TYPE
        from
            IPSDRDWH.IPSDR_FACT_TB as ftb
        left join 
            IPSDRDWH.CELL_TB as ctb on ftb.LOC_FK = ctb.CELL_ID
        left join 
            IPSDRDWH.MOBILE_SUBSCRIBER_TB as mstb on ftb.IMSI_FK = mstb.MOBSUBS_ID
        where 
            ctb.LATITUDE is not null and -- also longitude
            mstb.MOBSUBS_ID is not null and
            START_TIME_LOCAL is not null
    )
select * from call2g
union
select * from call3g
union
select * from internet
) tot
-- group by  (UNIX_TIMESTAMP(time_stamp) + r) DIV 30 
;
