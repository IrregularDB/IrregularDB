package config;

import compression.timestamp.TimestampCompressionModelType;
import compression.value.ValueCompressionModelType;

import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.*;
import java.util.function.Predicate;
import java.util.stream.Collectors;

public class ConfigProperties extends Properties{

    public static boolean isTest = false;
    private static ConfigProperties INSTANCE;

    private final Map<String, Integer> timestampErrorBounds = new HashMap<>();
    private final Map<String, Float> valueErrorBounds = new HashMap<>();

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
        parseAllTimeStampErrorBounds();
        parseAllValueErrorBounds();
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

    public List<TimestampCompressionModelType> getTimeStampModels(){
        return Arrays.stream(getProperty("model.timestamp.types").split(","))
                .map(String::trim)
                .filter(Predicate.not(String::isEmpty))
                .map(TimestampCompressionModelType::valueOf)
                .collect(Collectors.toList());
    }

    public Integer getTimestampModelErrorBound(){
        return Integer.parseInt(getProperty("model.timestamp.threshold"));
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

    public Optional<Integer> getTimeStampErrorBoundForTimeSeriesTagIfExists(String tag){
        if (this.timestampErrorBounds.containsKey(tag)){
            return Optional.of(this.timestampErrorBounds.get(tag));
        } else {
            return Optional.empty();
        }
    }

    public Optional<Float> getValueErrorBoundForTimeSeriesTagIfExists(String tag){
        if (this.timestampErrorBounds.containsKey(tag)){
            return Optional.of(this.valueErrorBounds.get(tag));
        } else {
            return Optional.empty();
        }
    }

    /* Private methods */
    private void parseAllTimeStampErrorBounds(){
        for (Enumeration<?> e = propertyNames(); e.hasMoreElements(); ) {
            String name = (String)e.nextElement();

            if (name.startsWith("model.timestamp.threshold.")) {
                String value = getProperty(name);
                this.timestampErrorBounds.put(name, Integer.parseInt(value));
            }
        }
    }

    private void parseAllValueErrorBounds(){
        for (Enumeration<?> e = propertyNames(); e.hasMoreElements(); ) {
            String name = (String)e.nextElement();
            if (name.startsWith("model.value.errorbound.")) {
                String value = getProperty(name);
                this.valueErrorBounds.put(name, Float.parseFloat(value));
            }
        }
    }
}
