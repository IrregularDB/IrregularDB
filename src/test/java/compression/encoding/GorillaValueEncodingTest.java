package compression.encoding;

import compression.utility.BitBuffer;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class GorillaValueEncodingTest {
    @Test
    void encode() {
        List<Float> values = List.of(1.0F, 1.0F, 1.0F, 2.0F, 3.0F, 10.0F);

        BitBuffer encode = GorillaValueEncoding.encode(values);
    }

    @Test
    void decode() {
    }
}