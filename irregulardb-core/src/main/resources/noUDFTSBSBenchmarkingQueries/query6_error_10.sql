\timing
--value := 40000
--error_bound:= 0.1 => 10%
--lowerBound := 36000
--upperBound := 44000
--segmetnSummary := TRUE|FALSE both are covered

select * from (
                  select (decompresssegment(segment)).* from segment
                  where segment.time_series_id = @6-1
                    AND (segment.minvalue is null OR (
                      segment.minvalue <= 44000
                    AND segment.maxvalue >= 36000
                      ))
              ) dp where 36000 <= dp.value and dp.value <= 44000
;