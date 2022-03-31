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

    private final Map<String, Integer> timestampThresholds = new HashMap<>();
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
        parseAllTimestampThresholds();
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

    public List<TimestampCompressionModelType> getTimestampModels(){
        return Arrays.stream(getProperty("model.timestamp.types").split(","))
                .map(String::trim)
                .filter(Predicate.not(String::isEmpty))
                .map(TimestampCompressionModelType::valueOf)
                .collect(Collectors.toList());
    }

    public Integer getTimestampModelThreshold(){
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

    public Integer getTimeStampThresholdForTimeSeriesTag(String tag){
        if (this.timestampThresholds.containsKey(tag)){
            return this.timestampThresholds.get(tag);
        } else {
            return this.getTimestampModelThreshold();
        }
    }

    public Float getValueErrorBoundForTimeSeriesTag(String tag){
        if (this.timestampThresholds.containsKey(tag)){
            return this.valueErrorBounds.get(tag);
        } else {
            return this.getValueModelErrorBound();
        }
    }

    /* Private methods */
    private void parseAllTimestampThresholds(){
        for (Enumeration<?> e = propertyNames(); e.hasMoreElements(); ) {
            String name = (String)e.nextElement();

            if (name.startsWith("model.timestamp.threshold.")) {
                String value = getProperty(name);
                this.timestampThresholds.put(name, Integer.parseInt(value));
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
