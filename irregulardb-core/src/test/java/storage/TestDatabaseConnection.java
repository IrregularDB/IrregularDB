package storage;

import records.Segment;

public class TestDatabaseConnection implements DatabaseConnection{

    @Override
    public void insertSegment(Segment segment) {
    }

    @Override
    public int getTimeSeriesId(String timeSeriesTag) {
        return switch (timeSeriesTag) {
            case "key1" -> 1;
            case "key2" -> 2;
            default -> throw new IllegalArgumentException("Not supported");
        };
    }
}
