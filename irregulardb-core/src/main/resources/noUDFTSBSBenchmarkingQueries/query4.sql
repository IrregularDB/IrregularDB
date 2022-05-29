--errorBound := 0
--Summary := TRUE|FALSE both are covered
-- minValue := 90
--maxValue := 2000000000 (inf)

select * from (
                  select (decompresssegment(segment)).* from segment
                  where segment.time_series_id = @4-1
                    AND (segment.minvalue is null
                     OR (
                      segment.minvalue <= 2000000000
                    AND segment.maxvalue >= 90)
                      )
              ) dp where 90 <= dp.value and dp.value <= 2000000000
;