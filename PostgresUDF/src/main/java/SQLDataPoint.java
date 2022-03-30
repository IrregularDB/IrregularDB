//import org.postgresql.pljava.annotation.BaseUDT;
//import org.postgresql.pljava.annotation.SQLType;
//
//import java.sql.SQLData;
//import java.sql.SQLException;
//import java.sql.SQLInput;
//import java.sql.SQLOutput;
//
//@BaseUDT(delimiter = ',')
//public class SQLDataPoint implements SQLData {
//
//    private int timeSeriesId;
//    private long timestamp;
//    private float value;
//
//    public SQLDataPoint(){
//
//    }
//
//    public SQLDataPoint(int timeSeriesId, long timestamp, float value) {
//        this.timeSeriesId = timeSeriesId;
//        this.timestamp = timestamp;
//        this.value = value;
//    }
//
//
//    @Override
//    public String getSQLTypeName() throws SQLException {
//        return "SQLDataPoint";
//    }
//
//    @Override
//    public void readSQL(SQLInput sqlInput, String s) throws SQLException {
//        this.timeSeriesId = sqlInput.readInt();
//        this.timestamp = sqlInput.readLong();
//        this.value = sqlInput.readFloat();
//    }
//
//    @Override
//    public void writeSQL(SQLOutput sqlOutput) throws SQLException {
//        sqlOutput.writeInt(timeSeriesId);
//        sqlOutput.writeLong(timestamp);
//        sqlOutput.writeFloat(value);
//    }
//
//    public static SQLDataPoint parse(String value, String typeName){
//        if (!typeName.equals("SQLDataPoint")) {
//            System.out.println("Something bad happened here");
//            return null;
//        }else{
//            return new SQLDataPoint(-1,-1,-1);
//        }
//    }
//
//
//    @Override
//    public String toString() {
//        return timeSeriesId +
//                "," + timestamp +
//                "," + value;
//    }
//}
