package compression.value;

import compression.timestamp.DeltaDeltaTimestampCompressionModel;
import compression.timestamp.RegularTimestampCompressionModel;
import compression.timestamp.SIDiffTimestampCompressionModel;
import compression.timestamp.TimestampCompressionModel;

import java.util.List;

public class TimestampCompressionModelsWrapper {
    private RegularTimestampCompressionModel regular;
    private SIDiffTimestampCompressionModel siDiff;
    private DeltaDeltaTimestampCompressionModel deltaDelta;

    public TimestampCompressionModelsWrapper(List<TimestampCompressionModel> timestampCompressionModels) {
        for (TimestampCompressionModel timestampCompressionModel : timestampCompressionModels) {
            switch (timestampCompressionModel.getTimestampCompressionModelType()) {
                case REGULAR -> regular = ((RegularTimestampCompressionModel) timestampCompressionModel);
                case SIDIFF -> siDiff = ((SIDiffTimestampCompressionModel) timestampCompressionModel);
                case DELTADELTA -> deltaDelta = ((DeltaDeltaTimestampCompressionModel) timestampCompressionModel);
                default -> throw new IllegalArgumentException("Are you missing a timesmapCompressionModelType in the switch in TimestampCompressionModelsWrapper");
            }
        }
    }

    public RegularTimestampCompressionModel getRegular() {
        return regular;
    }

    public SIDiffTimestampCompressionModel getSiDiff() {
        return siDiff;
    }

    public DeltaDeltaTimestampCompressionModel getDeltaDelta() {
        return deltaDelta;
    }
}
