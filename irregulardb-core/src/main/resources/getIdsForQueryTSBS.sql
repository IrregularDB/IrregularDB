-- NO1
select * from timeseries where
        tag like '/host_2/diskio/writes.csv'
;


-- NO2
select * from timeseries where
        tag like '/host_0/cpu/usage_idle.csv'
                            OR tag like '/host_0/mem/used_percent.csv'
                            OR tag like '/host_1/cpu/usage_idle.csv'
                            OR tag like '/host_1/mem/used_percent.csv'
                            OR tag like '/host_2/cpu/usage_idle.csv'
;

-- NO3
-- all timeseries

--NO4
select * from timeseries where
        tag like '/host_0/cpu/usage_idle.csv'
;


--NO5
select * from timeseries where
        tag like '/host_1/postgresl/deadlocks.csv'
;


--NO6
select * from timeseries where
        tag like '/host_2/net/bytes_recv.csv'
;

--NO7
select * from timeseries where
        tag like '/host_0/redis/sync_full.csv'
;
