\timing
--value := 40000
--error_bound:= 0.01 => 1%
--lowerBound := 39600
--upperBound := 40400
--segmetnSummary := TRUE|FALSE both are covered

select * from (
                  select (decompresssegment(segment)).* from segment
                  where segment.time_series_id = @6-1
                    AND (segment.minvalue is null OR (
                      segment.minvalue <= 40400
                    AND segment.maxvalue >= 39600
                      ))
              ) dp where 39600 <= dp.value and dp.value <= 40400
;