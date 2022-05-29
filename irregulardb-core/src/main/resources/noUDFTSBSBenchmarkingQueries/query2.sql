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






select ((bucketNumber * 300000) + 1303132933000)      as startTime,
       (bucketNumber * 300000 + 1303132933000 + 300000) as endTime,
       max(value)
from (
         select id,
                epochTime,
                value,
                CAST((epochTime - 1303132933000) / 300000 as BIGINT) as bucketNumber
         from (
                  select (timestampRangeQuery(id, 0, 1451649600000, 0)).*
                  from timeseries
                  where id in (161,162,163,164,165)
              ) res
     ) dpWithBucketNumber group by bucketNumber;