package storage;

public class DatabaseConnectionFactory {
    public DatabaseConnection createDataBaseConnection(){
        return new PostgresConnection();
    }
}
