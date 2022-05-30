\timing
--timestamp:= 1303134414000
--thres:= 500
--lowerBound := 1303134413500
--upperBound := 1303134414500

select * from (
    select (decompresssegment(segment)).* from segment
                  where segment.time_series_id = @5-1
                  and segment.start_time <= 1303134414500
                  AND 1303134413500 <= (segment.start_time + segment.end_time)
    ) dp where 1303134413500 <= dp.timestamp and dp.timestamp <= 1303134414500
;
