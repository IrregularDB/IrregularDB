package compression.utility;

public class PercentageError {

    public static boolean isWithinErrorBound(float approximation, float realValue, float errorBound) {
        float percentageError = calculatePercentageError(approximation, realValue);

        return percentageError <= errorBound;
    }

    public static float calculatePercentageError(float approximation, float realValue) {
        if (approximation == realValue) {
            return 0.0F;
        }

        float difference = approximation - realValue;
        return Math.abs(difference / realValue) * 100;
    }


}
