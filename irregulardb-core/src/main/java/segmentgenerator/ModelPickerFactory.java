package segmentgenerator;

import config.ConfigProperties;

public class ModelPickerFactory {

    public enum ModelPickerType {
        ModelPickerGreedy,
        ModelPickerBruteForce;
    }

    private final ModelPickerType modelPickerType;

    public ModelPickerFactory() {
        modelPickerType = ConfigProperties.getInstance().getModelPickerType();
    }

    public ModelPicker getModelPicker() {
        return initializeModelPickerByType(this.modelPickerType);
    }

    private static ModelPicker initializeModelPickerByType(ModelPickerType modelPickerType) {
        return switch (modelPickerType) {
            case ModelPickerGreedy -> new ModelPickerGreedy();
            case ModelPickerBruteForce -> new ModelPickerBruteForce();
            default -> throw new IllegalArgumentException("Illegal model picker type" + modelPickerType.name());
        };
    }
}
