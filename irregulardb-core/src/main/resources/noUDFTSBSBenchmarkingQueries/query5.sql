\timing
--timestamp:= 1454075479700
--thres:= 0
--lowerBound := 1454075479700
--upperBound := 1454075479700

select * from (
    select (decompresssegment(segment)).* from segment
                  where segment.time_series_id = @5-1
                  and segment.start_time <= 1454075479700
                  AND 1454075479700 <= (segment.start_time + segment.end_time)
    ) dp where 1454075479700 <= dp.timestamp and dp.timestamp <= 1454075479700
;
