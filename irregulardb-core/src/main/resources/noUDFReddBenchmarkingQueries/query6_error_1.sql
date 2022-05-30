\timing
--value := 318.5475612997497
--error_bound:= 0.01 => 1%
--lowerBound := 315.3620856867522
--upperBound := 321.7330369127472
--segmetnSummary := TRUE|FALSE both are covered

select * from (
                  select (decompresssegment(segment)).* from segment
                  where segment.time_series_id = @6-1
                    AND (segment.minvalue is null OR (
                      segment.minvalue <= 321.7330369127472
                    AND segment.maxvalue >= 315.3620856867522
                      ))
              ) dp where 315.3620856867522 <= dp.value and dp.value <= 321.7330369127472
;