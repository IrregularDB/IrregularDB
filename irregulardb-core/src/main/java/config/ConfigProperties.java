package config;

import compression.timestamp.TimestampCompressionModelType;
import compression.value.ValueCompressionModelType;
import segmentgenerator.ModelPickerFactory;

import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.*;
import java.util.function.Predicate;
import java.util.stream.Collectors;

public class ConfigProperties extends Properties {

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

    private ConfigProperties(String path) {
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

    public int getConfiguredNumberOfWorkingSets() {
        String workingsets = getProperty("workingsets");
        if (workingsets == null) {
            // Defaults to 1
            return 1;
        }

        return Integer.parseInt(workingsets);
    }

    public List<String> getFolderSources(){
        String folderSource = getProperty("source.folder");
        if (folderSource == null){
            return Collections.emptyList();
        }

        List<String> folderSources = Arrays.stream(folderSource.trim().split(","))
                .map(String::trim).toList();

        List<String> csvSources = new ArrayList<>();

        for (String folderPath : folderSources){
            File[] files = new File(folderPath).listFiles();
            if (files != null) {
                for (File file : files){
                    if (file.isFile()){
                        csvSources.add(file.getName());
                    }
                }
            }
        }

        return csvSources;
    }

    public List<String> getCsvSources() {
        String csvSource = getProperty("source.csv");
        if (csvSource == null) {
            // no sources -> return empty list
            return Collections.emptyList();
        }

        return Arrays.stream(csvSource.trim().split(","))
                .map(String::trim)
                .collect(Collectors.toList());
    }

    public List<ValueCompressionModelType> getValueModels() {

        String valueTypes = getProperty("model.value.types");

        if (valueTypes == null) {
            return new ArrayList<>(List.of(ValueCompressionModelType.values()));
        }

        return Arrays.stream(valueTypes.split(","))
                .map(String::trim)
                .filter(Predicate.not(String::isEmpty))
                .map(ValueCompressionModelType::valueOf)
                .collect(Collectors.toList());
    }

    public List<TimestampCompressionModelType> getTimestampModels() {

        String timestampTypes = getProperty("model.timestamp.types");

        if (timestampTypes == null) {
            return new ArrayList<>(List.of(TimestampCompressionModelType.values()));
        }

        return Arrays.stream(timestampTypes.split(","))
                .map(String::trim)
                .filter(Predicate.not(String::isEmpty))
                .map(TimestampCompressionModelType::valueOf)
                .collect(Collectors.toList());
    }

    public Integer getTimestampModelThreshold() {
        return Integer.parseInt(getProperty("model.timestamp.threshold", "0"));
    }

    public float getValueModelErrorBound() {
        return Float.parseFloat(getProperty("model.value.error_bound", "0.0"));
    }

    public String getJDBConnectionString() {
        return getProperty("database.jdbc.connection_string");
    }

    public int getSocketDataReceiverSpawnerPort() {
        return Integer.parseInt(getProperty("source.socket.port", "4672"));
    }

    public int getModelLengthBound() {
        return Integer.parseInt(getProperty("model.length_bound", "50"));
    }

    public Integer getTimeStampThresholdForTimeSeriesTag(String tag) {
        if (this.timestampThresholds.containsKey(tag)) {
            return this.timestampThresholds.get(tag);
        } else {
            return this.getTimestampModelThreshold();
        }
    }

    public Float getValueErrorBoundForTimeSeriesTag(String tag) {
        if (this.timestampThresholds.containsKey(tag)) {
            return this.valueErrorBounds.get(tag);
        } else {
            return this.getValueModelErrorBound();
        }
    }

    public boolean populateSegmentSummary() {
        return Boolean.parseBoolean(getProperty("model.segment.compute.summary", "false"));
    }

    public ModelPickerFactory.ModelPickerType getModelPickerType(){
        String modelPickerType = getProperty("model.picker");
        if (modelPickerType == null) {
            return ModelPickerFactory.ModelPickerType.GREEDY;
        }
        return ModelPickerFactory.ModelPickerType.valueOf(modelPickerType);
    }

    /* Private methods */
    private void parseAllTimestampThresholds() {
        for (Enumeration<?> e = propertyNames(); e.hasMoreElements(); ) {
            String name = (String) e.nextElement();

            if (name.startsWith("model.timestamp.threshold.")) {
                String value = getProperty(name);
                this.timestampThresholds.put(name, Integer.parseInt(value));
            }
        }
    }

    private void parseAllValueErrorBounds() {
        for (Enumeration<?> e = propertyNames(); e.hasMoreElements(); ) {
            String name = (String) e.nextElement();
            if (name.startsWith("model.value.errorbound.")) {
                String value = getProperty(name);
                this.valueErrorBounds.put(name, Float.parseFloat(value));
            }
        }
    }
}
