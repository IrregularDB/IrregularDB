package compression.utility;

import records.ValueTimeStampModelPair;

public class ModelTypeUtil {
    public static short combineTwoModelTypes(byte valueModelType, byte timestampModelType){
        if (valueModelType < 0 || timestampModelType < 0) {
            throw new IllegalArgumentException("The model types ids must be positive");
        }
        return (short) ((valueModelType << 8) | timestampModelType);
    }

    public static ValueTimeStampModelPair combinedModelTypesToIndividual(short combined) {
        final short leastSignificantByteMask = 0b0000000011111111;
        return new ValueTimeStampModelPair(combined >> 8, combined & leastSignificantByteMask);
    }


}
