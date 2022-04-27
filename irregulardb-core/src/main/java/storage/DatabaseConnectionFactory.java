package storage;

public class DatabaseConnectionFactory {
    //TODO: Should be general based on config?
    public DatabaseConnection createDataBaseConnection(){
        return new PostgresConnection();
    }
}
