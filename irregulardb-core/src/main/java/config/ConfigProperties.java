package config;

import compression.timestamp.TimeStampCompressionModelType;
import compression.value.ValueCompressionModelType;

import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import java.util.function.Predicate;
import java.util.stream.Collectors;

public class ConfigProperties extends Properties{

    private static ConfigProperties INSTANCE;
    public static boolean isTest = false;

    public static ConfigProperties getInstance() {
        if (INSTANCE != null) {
            return INSTANCE;
        }

        if (!isTest) {
            INSTANCE = new ConfigProperties("irregulardb-core/src/main/resources/config.properties");
        } else {
            INSTANCE = new ConfigProperties("src/test/resources/config.properties");
        }
        return INSTANCE;
    }

    private ConfigProperties(String path){
        String property = System.getProperty("user.dir");
        File file = new File(path);
        try {
            FileReader fileReader = new FileReader(file);
            load(fileReader);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public int getConfiguredNumberOfWorkingSets(){
        String workingsets = getProperty("workingsets");
        if (workingsets == null) {
            throw new MissingConfigPropertyException("workingsets");
        }

        return Integer.parseInt(workingsets);
    }

    public List<String> getCsvSources(){
        String csvSource = getProperty("source.csv");
        if (csvSource == null) {
            throw new MissingConfigPropertyException("source.csv");
        }

        return Arrays.stream(csvSource.trim().split(","))
                .map(String::trim)
                .collect(Collectors.toList());
    }

    public List<ValueCompressionModelType> getValueModels(){
        return Arrays.stream(getProperty("model.value.types").split(","))
                .map(String::trim)
                .filter(Predicate.not(String::isEmpty))
                .map(ValueCompressionModelType::valueOf)
                .collect(Collectors.toList());
    }

    public List<TimeStampCompressionModelType> getTimeStampModels(){
        return Arrays.stream(getProperty("model.timestamp.types").split(","))
                .map(String::trim)
                .filter(Predicate.not(String::isEmpty))
                .map(TimeStampCompressionModelType::valueOf)
                .collect(Collectors.toList());
    }

    public float getTimeStampModelErrorBound(){
        return Float.parseFloat(getProperty("model.timestamp.errorbound"));
    }

    public float getValueModelErrorBound(){
        return Float.parseFloat(getProperty("model.value.errorbound"));
    }

    public String getJDBConnectionString(){
        return getProperty("database.jdbc.connectionstring");
    }

    public int getSocketDataReceiverSpawnerPort(){
        return Integer.parseInt(getProperty("source.socket.port"));
    }

    public int getValueModelLengthBound(){
        return Integer.parseInt(getProperty("model.value.length_bound"));
    }
}