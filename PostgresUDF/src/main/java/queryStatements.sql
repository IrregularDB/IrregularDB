--How to make a range query
My range [3, 5]
3 < max and min < 5

select (decompressSegment(segment)).* from segment
    where start_time < @myQueryTime AND @myQueryTime < (start_time + segment.end_time);


--range [3,5]
select sum.minvalue, sum.maxvalue, seg.* from segment seg
    join segmentsummary sum on seg.time_series_id = sum.time_series_id and seg.start_time = sum.start_time
    where 3 < sum.maxvalue AND sum.minvalue  < 5
;
