package utility;

import java.util.HashMap;
import java.util.Map;

public class Stopwatch {
    private static Long initialStartTime;
    private static final Map<String, Long> startTimesByTag;

    private Stopwatch() {
    }

    static{ // Static constructor
        startTimesByTag = new HashMap<>();
        initialStartTime = null;
    }

    public static void setInitialStartTime() {
        initialStartTime = System.currentTimeMillis();
    }

    public static void putStartTime(String tag) {
        startTimesByTag.put(tag, System.currentTimeMillis());
    }

    public static void getDurationForTag(String tag) {
        long startTime = startTimesByTag.remove(tag);

        long endTime = System.currentTimeMillis();
        double duration = endTime - startTime;
        System.out.println(tag + " is finished. Duration: " + duration/1000 + "sec");

        if (startTimesByTag.isEmpty()) {
            double runTime = endTime - initialStartTime;
            System.out.println("Total ingestion time: " + runTime/1000 + "sec");
        }
    }
}
