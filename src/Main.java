import records.DataPoint;

public class Main {

    public static void main(String[] args) {
        System.out.println("Hello World!");
        DataPoint dataPoint = new DataPoint(5, 5.5);
        DataPoint dataPoint1 = new DataPoint(5, 5.5);

        System.out.println(dataPoint.equals(dataPoint1));

        System.out.println(dataPoint.timestamp());
        System.out.println(dataPoint.value());
    }

}
