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
            // Defaults to 1
            return 1;
        }

        return Integer.parseInt(workingsets);
    }

    public List<String> getCsvSources(){
        String csvSource = getProperty("source.csv");
        if (csvSource == null) {
            // no sources -> return empty list
            return new ArrayList<>();
        }

        return Arrays.stream(csvSource.trim().split(","))
                .map(String::trim)
                .collect(Collectors.toList());
    }

    public List<ValueCompressionModelType> getValueModels(){

        String valueTypes = getProperty("model.value.types");

        if (valueTypes == null){
            return new ArrayList<>(List.of(ValueCompressionModelType.values()));
        }

        return Arrays.stream(valueTypes.split(","))
                .map(String::trim)
                .filter(Predicate.not(String::isEmpty))
                .map(ValueCompressionModelType::valueOf)
                .collect(Collectors.toList());
    }

    public List<TimestampCompressionModelType> getTimestampModels(){

        String timestampTypes = getProperty("model.timestamp.types");

        if (timestampTypes == null){
            return new ArrayList<>(List.of(TimestampCompressionModelType.values()));
        }

        return Arrays.stream(timestampTypes.split(","))
                .map(String::trim)
                .filter(Predicate.not(String::isEmpty))
                .map(TimestampCompressionModelType::valueOf)
                .collect(Collectors.toList());
    }

    public Integer getTimestampModelThreshold(){
        String thresholdProperty = getProperty("model.timestamp.threshold");

        if (thresholdProperty == null){
            return 0;
        } else {
            return Integer.parseInt(thresholdProperty);
        }
    }

    public float getValueModelErrorBound(){
        String errorBoundProperty = getProperty("model.value.errorbound");

        if (errorBoundProperty == null){
            return 0.0f;
        } else {
            return Float.parseFloat(errorBoundProperty);
        }
    }

    public String getJDBConnectionString(){
        return getProperty("database.jdbc.connectionstring");
    }

    public int getSocketDataReceiverSpawnerPort(){
        return Integer.parseInt(getProperty("source.socket.port"));
    }

    public int getModelLengthBound(){
        String lengthBoundProperty = getProperty("model.length_bound");
        if (lengthBoundProperty == null){
            return 50;
        } else {
            return Integer.parseInt(lengthBoundProperty);
        }
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

    public boolean populateSegmentSummary(){
        String summaryProperty = getProperty("model.segment.compute.summary", "true");
        if (summaryProperty == null){
            return false;
        } else {
            return Boolean.parseBoolean(summaryProperty);
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
