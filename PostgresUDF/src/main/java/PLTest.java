import org.postgresql.pljava.annotation.Function;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class PLTest {

    @Function
    public static Integer test(Integer inputString){
        return inputString + 1;
    }

    @Function
    public static String hello(String toWhom) {
        return "Hello, " + toWhom + "!";
    }
//FOR ME: <>
    @Function
    public static Iterator<String> hellos(String toWhom) {
        ArrayList<String> stringArray = new ArrayList<>();

        for (int i = 0; i != 10; i++){
            stringArray.add(toWhom);
        }

        return stringArray.iterator();
    }
}
