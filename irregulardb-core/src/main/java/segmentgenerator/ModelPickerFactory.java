package segmentgenerator;

import config.ConfigProperties;

public class ModelPickerFactory {

    public enum ModelPickerType {
        GREEDY,
        BRUTE_FORCE;
    }

    private static final ModelPickerType modelPickerType = ConfigProperties.getInstance().getModelPickerType();


    public static ModelPicker getModelPicker() {
        return initializeModelPickerByType(modelPickerType);
    }

    private static ModelPicker initializeModelPickerByType(ModelPickerType modelPickerType) {
        return switch (modelPickerType) {
            case GREEDY -> new ModelPickerGreedy();
            case BRUTE_FORCE -> new ModelPickerBruteForce();
            default -> throw new IllegalArgumentException("Illegal model picker type" + modelPickerType.name());
        };
    }
}
