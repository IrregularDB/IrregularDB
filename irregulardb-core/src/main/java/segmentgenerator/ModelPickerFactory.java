package segmentgenerator;

public class ModelPickerFactory {

    public enum ModelPickerType {
        GREEDY,
        BRUTE_FORCE;
    }

    public static ModelPicker createModelPicker(ModelPickerType modelPickerType) {
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
