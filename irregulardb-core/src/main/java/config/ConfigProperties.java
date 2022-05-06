package config;

import compression.timestamp.TimestampCompressionModelType;
import compression.value.ValueCompressionModelType;
import records.Pair;
import segmentgenerator.ModelPickerFactory;
import utility.CSVFileGetter;

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

    /**
     * This method should never be called from a static context
     */
    public static ConfigProperties getInstance() {
        if (INSTANCE != null) {
            return INSTANCE;
        }

        if (!isTest) {
            INSTANCE = new ConfigProperties("irregulardb-core/src/main/resources/config.properties");
        } else {
            File configProperties = new File("./config.properties");
            if (configProperties.exists()) {
                INSTANCE = new ConfigProperties(configProperties.getAbsolutePath());
            } else {
                INSTANCE = new ConfigProperties("src/test/resources/config.properties");
            }
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
        String workingsets = getProperty("workingsets", "1");
        return Integer.parseInt(workingsets);
    }

    public List<Pair<File, String>> getCsvSourceFilesWithFileNameTag() {
        String csvSource = getProperty("source.csv");
        if (csvSource == null) {
            // no sources -> return empty list
            return Collections.emptyList();
        }

         return Arrays.stream(csvSource.trim().split(","))
                .map(String::trim)
                .map(File::new)
                .map(CSVFileGetter::getCsvFilesWithTag)
                .flatMap(map -> map.entrySet().stream())
                .map(entry -> new Pair<>(entry.getKey(), entry.getValue()))
                .toList();
    }

    /**
     * You should wrap the delimiter in " on both sides.
     */
    public String getCsvDelimiter() {
        // Defaults to ","
        String csvDelimiter = getProperty("source.csv.delimiter", ",").replace("\"", "");
        return csvDelimiter;
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
            return ModelPickerFactory.ModelPickerType.BRUTE_FORCE;
        }
        return ModelPickerFactory.ModelPickerType.valueOf(modelPickerType);
    }

    public int getJDBCBatchSize(){
        return Integer.parseInt(getProperty("database.jdbc.batch_size", "100"));
    }


    public int getMaxBufferSizeBeforeThrottle(){
        return Integer.parseInt(getProperty("workingset.max_buffer_size_before_throttle", "1000000"));
    }

    public int getReceiverCSVThrottleSleepTime(){
        return Integer.parseInt(getProperty("receiver.csv.throttle_sleep_time", "50"));
    }

    public boolean getModelValueErrorBoundStrict(){
        return Boolean.parseBoolean(getProperty("model.value.error_bound.strict", "true"));
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
