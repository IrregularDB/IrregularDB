\timing
select res.timeseriesid, res.timestamp,res.value  from (
                                                           select s.start_time + s.end_time as latestTimestamp, (decompresssegment(s)).*
                                                           from (
                                                               select time_series_id, max(start_time) as start_time from segment group by time_series_id
                                                               ) lastSegmentTime
                                                               join segment s on s.start_time = lastSegmentTime.start_time and
                                                               s.time_series_id = lastSegmentTime.time_series_id
                                                       ) res where res.latestTimestamp = res.timestamp
;