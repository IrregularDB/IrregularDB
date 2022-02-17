package config;

import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import java.util.stream.Collectors;

public class ConfigProperties extends Properties{

    public static final ConfigProperties INSTANCE = new ConfigProperties();

    private ConfigProperties(){
        File file = new File("src/main/resources/config.properties");
        try {
            FileReader fileReader = new FileReader(file);
            load(fileReader);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public int getConfiguredNumberOfWorkingSets(){
        return Integer.parseInt(getProperty("workingsets"));
    }

    public List<String> getCsvSources(){
        return Arrays.stream(getProperty("source.csv").trim().split(","))
                .map(String::trim)
                .collect(Collectors.toList());
    }

    public String test(){
        return getProperty("test");
    }
}
