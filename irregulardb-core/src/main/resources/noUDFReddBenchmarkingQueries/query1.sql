\timing
--startTime:= 1303002992000
--endTime := 1303046192000
-- bucketSize := 300000
select  ((dpWithBucket.bucketId * 300000) + 1303002992000) as startTime,
        (dpWithBucket.bucketId * 300000 + 1303002992000 + 300000) as endTime,
        cast(avg(value) as real) as maxValue
from (
         select *,
                CAST(((dp.timestamp - 1303002992000) / 300000) as BIGINT) as bucketId
         from (
                  select (decompresssegment(segment)).*
                  from segment
                  where segment.time_series_id = @1-1
                    and segment.start_time <= 1303046192000
                    AND 1303002992000 <= (segment.start_time + segment.end_time)
              ) dp
         where 1303002992000 <= dp.timestamp
           and dp.timestamp <= 1303046192000
     ) dpWithBucket group by dpWithBucket.bucketId
;