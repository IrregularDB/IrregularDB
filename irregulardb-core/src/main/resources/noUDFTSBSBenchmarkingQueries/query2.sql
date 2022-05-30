\timing
--startTime:= 1451606400000
--endTime := 1451649600000
-- bucketSize := 300000
select  ((dpWithBucket.bucketId * 300000) + 1451606400000) as startTime,
        (dpWithBucket.bucketId * 300000 + 1451606400000 + 300000) as endTime,
        cast(max(value) as real) as maxValue
from (
         select *,
                CAST(((dp.timestamp - 1451606400000) / 300000) as BIGINT) as bucketId
         from (
                  select (decompresssegment(segment)).*
                  from segment
                  where segment.time_series_id IN (@2-1,@2-2,@2-3,@2-4,@2-5)
                    and segment.start_time <= 1451649600000
                    AND 1451606400000 <= (segment.start_time + segment.end_time)
              ) dp
         where 1451606400000 <= dp.timestamp
           and dp.timestamp <= 1451649600000
     ) dpWithBucket group by dpWithBucket.bucketId
;
