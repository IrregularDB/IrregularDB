package scheduling;

import records.TimeSeriesReading;

import java.util.ArrayList;
import java.util.List;

public class TestWorkingSetFactory extends WorkingSetFactory{

    private List<TestWorkingSet> generatedWorkingSets = new ArrayList<>();

    @Override
    public WorkingSet generateWorkingSet() {
        TestWorkingSet testWorkingSet = new TestWorkingSet();
        this.generatedWorkingSets.add(testWorkingSet);
        return testWorkingSet;
    }

    public List<TestWorkingSet> getGeneratedWorkingSets() {
        return generatedWorkingSets;
    }

    public static class TestWorkingSet implements WorkingSet{
        private List<TimeSeriesReading> acceptedRecordings = new ArrayList<>();

        @Override
        public void accept(TimeSeriesReading timeSeriesReading) {
            acceptedRecordings.add(timeSeriesReading);
        }

        public List<TimeSeriesReading> getAcceptedRecordings() {
            return acceptedRecordings;
        }

        @Override
        public void run() {

        }
    }
}
