package segmentgenerator;

import config.ConfigProperties;

public class ModelPickerFactory {

    public enum ModelPickerType {
        MODEL_PICKER_GREEDY,
        MODEL_PICKER_BRUTE_FORCE;
    }

    private static final ModelPickerType modelPickerType = ConfigProperties.getInstance().getModelPickerType();


    public static ModelPicker getModelPicker() {
        return initializeModelPickerByType(modelPickerType);
    }

    private static ModelPicker initializeModelPickerByType(ModelPickerType modelPickerType) {
        return switch (modelPickerType) {
            case MODEL_PICKER_GREEDY -> new ModelPickerGreedy();
            case MODEL_PICKER_BRUTE_FORCE -> new ModelPickerBruteForce();
            default -> throw new IllegalArgumentException("Illegal model picker type" + modelPickerType.name());
        };
    }
}
