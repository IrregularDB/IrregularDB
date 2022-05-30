\timing
--value := 318.5475612997497
--error_bound:= 0.1 => 10%
--lowerBound := 286.6928051697747
--upperBound := 350.4023174297247
--segmetnSummary := TRUE|FALSE both are covered

select * from (
                  select (decompresssegment(segment)).* from segment
                  where segment.time_series_id = @6-1
                    AND (segment.minvalue is null OR (
                      segment.minvalue <= 350.4023174297247
                    AND segment.maxvalue >= 286.6928051697747
                      ))
              ) dp where 286.6928051697747 <= dp.value and dp.value <= 350.4023174297247
;