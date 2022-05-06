package utility;

import java.io.File;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class CSVFileGetter {

    public static Map<File, String> getCsvFilesWithTag(File sourceDirectory) {
        return getCsvFiles(sourceDirectory, "");
    }




    private static Map<File, String> getCsvFiles(File sourceDirectory, String filePath){
        HashMap<File, String> result = new HashMap<>();

        File[] subFiles = sourceDirectory.listFiles();

        if (subFiles == null){
            if (sourceDirectory.isFile())
                result.put(sourceDirectory, filePath + sourceDirectory.getName());
            return result;
        }

        for (File file : subFiles){
            if (file.isDirectory()){
                result.putAll(getCsvFiles(file, filePath + "/" + file.getName()));
            } else if (file.isFile()) {
                result.put(file, filePath + "/" + file.getName());
            }
        }
        return result;
    }



}
