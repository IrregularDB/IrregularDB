package compression.utility;

public class PercentageError {

    public static boolean isWithinErrorBound(double approximation, double realValue, double errorBound) {
        double percentageError = calculatePercentageError(approximation, realValue);

        return percentageError <= errorBound;
    }

    public static double calculatePercentageError(double approximation, double realValue) {
        if (approximation == realValue) {
            return 0.0;
        }

        double difference = approximation - realValue;
        return Math.abs(difference / realValue) * 100;
    }


}
