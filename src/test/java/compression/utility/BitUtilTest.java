package compression.utility;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;


class BitUtilTest {

    @Test
    public void testInt2Bits(){
        int testInt = 7;

        Assertions.assertEquals("111", BitUtil.int2Bits(testInt));
    }

}