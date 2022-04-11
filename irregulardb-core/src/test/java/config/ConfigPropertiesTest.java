package config;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.util.*;

import static org.junit.jupiter.api.Assertions.*;

class ConfigPropertiesTest {

    @BeforeAll
    public static void setup(){
        ConfigProperties.isTest = true;
    }

    @Test
    public void testFolderSources(){
        ConfigProperties instance = ConfigProperties.getInstance();
        instance.setProperty("source.csv", "./src/test/resources/testFolder");

        List<File> sources = new ArrayList<>(ConfigProperties.getInstance().getCsvSources());
        Collections.sort(sources); // Sorted alphabetically

        assertEquals(2, sources.size());
        assertEquals(new File(".\\src\\test\\resources\\testFolder\\test1.csv"), sources.get(0));
        assertEquals(new File(".\\src\\test\\resources\\testFolder\\test2.csv"), sources.get(1));
    }

    @Test
    public void testMixedSources(){
        ConfigProperties instance = ConfigProperties.getInstance();
        instance.setProperty("source.csv", "./src/test/resources/testFolder, " +
                "src/test/resources/integration-test/01.csv");

        List<File> sources = new ArrayList<>(ConfigProperties.getInstance().getCsvSources());
        Collections.sort(sources); // Sorted alphabetically

        assertEquals(3, sources.size());
        assertEquals(new File(".\\src\\test\\resources\\testFolder\\test1.csv"), sources.get(0));
        assertEquals(new File(".\\src\\test\\resources\\testFolder\\test2.csv"), sources.get(1));
        assertEquals(new File("src/test/resources/integration-test/01.csv"), sources.get(2));


    }



}