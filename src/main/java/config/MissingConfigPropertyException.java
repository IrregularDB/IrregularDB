package config;

public class MissingConfigPropertyException extends RuntimeException {
    public MissingConfigPropertyException(String propertyName) {
        super("The following property was not specified in the config:" + propertyName);
    }
}
