package storage;

public class TestDatabaseConnectionFactory extends DatabaseConnectionFactory {

    @Override
    public DatabaseConnection createDataBaseConnection() {
        return new TestDatabaseConnection();
    }

}
