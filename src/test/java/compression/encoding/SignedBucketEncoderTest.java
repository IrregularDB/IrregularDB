package compression.encoding;

import compression.timestamp.DeltaDeltaTimeStampCompression;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class SignedBucketEncoderTest {

    private SignedBucketEncoder signedBucketEncoder = new SignedBucketEncoder();

    @Test
    void testSignedBucketEncoderEncodeNegativeValues(){
        int valueToEncode = -8;

        signedBucketEncoder.encode(List.of(valueToEncode));

        signedBucketEncoder.

    }

}