package compression.value;

import java.util.List;

public class ValueCompressionModelsWrapper {
    private PMCMeanValueCompressionModel pmcMean;
    private SwingValueCompressionModel swing;
    private GorillaValueCompressionModel gorilla;

    public ValueCompressionModelsWrapper(List<ValueCompressionModel> valueCompressionModelList) {
        for (ValueCompressionModel valueCompressionModel : valueCompressionModelList) {
            switch (valueCompressionModel.getValueCompressionModelType()) {
                case PMC_MEAN -> pmcMean = (PMCMeanValueCompressionModel) valueCompressionModel;
                case SWING -> swing = (SwingValueCompressionModel) valueCompressionModel;
                case GORILLA -> gorilla = (GorillaValueCompressionModel) valueCompressionModel;
                default -> throw new IllegalArgumentException("Are you missing a type here");

            }
        }
    }

    public PMCMeanValueCompressionModel getPmcMean() {
        return pmcMean;
    }

    public SwingValueCompressionModel getSwing() {
        return swing;
    }

    public GorillaValueCompressionModel getGorilla() {
        return gorilla;
    }
}
