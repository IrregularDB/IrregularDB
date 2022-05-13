-- NO1
select * from timeseries where
        tag like '/house_4_0.823/channel_7_sorted.csv'
;

-- NO2
select * from timeseries where
        tag like '/house_4_0.823/channel_3_sorted.csv'
    OR tag like '/house_4_0.823/channel_4_sorted.csv'
    OR tag like '/house_4_0.823/channel_5_sorted.csv'
    OR tag like '/house_4_0.823/channel_6_sorted.csv'
    OR tag like '/house_4_0.823/channel_7_sorted.csv'
;

-- NO3
-- all timeseries

--NO4
select * from timeseries where
        tag like '/house_1-8.879/channel_3_sorted.csv'
;

--NO5
select * from timeseries where
        tag like '/house_5-4.322/channel_12_sorted.csv'
;


--NO6
select * from timeseries where
        tag like '/house_6-5.716/channel_16_sorted.csv'
;

--NO7
select * from timeseries where
        tag like '/house_2-6.416/channel_4_sorted.csv'
;

