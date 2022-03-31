import org.postgresql.pljava.annotation.Function;

import java.util.ArrayList;
import java.util.Iterator;

public class PLTest {

    @Function
    public static String hello(String toWhom) {
        return "Hello, " + toWhom + "!";
    }
}
