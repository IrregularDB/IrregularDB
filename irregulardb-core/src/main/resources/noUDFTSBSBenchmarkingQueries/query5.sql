--timestamp:= 1454075479700
--thres:= 500
--lowerBound := 1454075479200
--upperBound := 1454075480200

select * from (
    select (decompresssegment(segment)).* from segment
                  where segment.time_series_id = @5-1
                  and segment.start_time <= 1454075480200
                  AND 1454075479200 <= (segment.start_time + segment.end_time)
    ) dp where 1454075479200 <= dp.timestamp and dp.timestamp <= 1454075480200
;
