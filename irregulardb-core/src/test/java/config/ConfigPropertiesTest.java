package config;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class ConfigPropertiesTest {

    @BeforeAll
    public static void setup(){
        ConfigProperties.isTest = true;
    }

    @Test
    public void testFolderSources(){
        List<String> folderSources = ConfigProperties.getInstance().getFolderSources();

        assertEquals(10, folderSources.size());
    }

}