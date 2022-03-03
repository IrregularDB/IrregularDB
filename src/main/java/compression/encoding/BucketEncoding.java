package compression.encoding;

import compression.utility.*;
import compression.utility.BitBuffer.BitBuffer;
import compression.utility.BitBuffer.BitBufferNew;
import compression.utility.BitStream.BitStream;

import java.util.ArrayList;
import java.util.List;

public class BucketEncoding {

    private static final String SAME_VALUE_ENCODING = "00";
    private static final String BUCKET_1_CONTROL_BITS = "01";
    private static final String BUCKET_2_CONTROL_BITS = "10";
    private static final String BUCKET_3_CONTROL_BITS = "11";

    private static final int BUCKET_1_BIT_SIZE = 9;
    private static final int BUCKET_2_BIT_SIZE = 16;
    private static final int BUCKET_3_BIT_SIZE = 31;
    private static final int AMT_CONTROL_BITS = 2;

    /**
     * @param readings we only support positive numbers
     */
    public static BitBuffer encode(List<Integer> readings) {
        // We finish the byte with 1's as we can then in the decoding detect end of stream
        BitBuffer bitBuffer = new BitBufferNew(true);
        Integer previousReading = null;
        for (Integer reading : readings) {
            encodeReading(bitBuffer, reading, previousReading);
            previousReading = reading;
        }

        return bitBuffer;
    }

    private static void encodeReading(BitBuffer bitBuffer, Integer reading, Integer prevReading) {
        if (reading.equals(prevReading)) {
            writeControlBitToBuffer(SAME_VALUE_ENCODING, bitBuffer);
        } else {
            encodeNumber(reading, bitBuffer);
        }
    }

    private static void encodeNumber(Integer reading, BitBuffer bitBuffer) {
        int amtSignificantBits = Integer.SIZE - Integer.numberOfLeadingZeros(reading);

        if (amtSignificantBits <= BUCKET_1_BIT_SIZE) {
            writeControlBitToBuffer(BUCKET_1_CONTROL_BITS, bitBuffer);
            bitBuffer.writeIntUsingNBits(reading, BUCKET_1_BIT_SIZE);
        } else if (amtSignificantBits <= BUCKET_2_BIT_SIZE) {
            writeControlBitToBuffer(BUCKET_2_CONTROL_BITS, bitBuffer);
            bitBuffer.writeIntUsingNBits(reading, BUCKET_2_BIT_SIZE);
        } else if (amtSignificantBits <= BUCKET_3_BIT_SIZE) {
            writeControlBitToBuffer(BUCKET_3_CONTROL_BITS, bitBuffer);
            bitBuffer.writeIntUsingNBits(reading, BUCKET_3_BIT_SIZE);
        } else {
            throw new IllegalArgumentException("Amount of bits greater than bucket allows (you probably tried to insert a negative number)");
        }
    }

    private static void writeControlBitToBuffer(String controlBits, BitBuffer bitBuffer) {
        switch (controlBits) {
            case SAME_VALUE_ENCODING -> {
                bitBuffer.writeFalseBit();
                bitBuffer.writeFalseBit();
            }
            case BUCKET_1_CONTROL_BITS -> {
                bitBuffer.writeFalseBit();
                bitBuffer.writeTrueBit();
            }
            case BUCKET_2_CONTROL_BITS -> {
                bitBuffer.writeTrueBit();
                bitBuffer.writeFalseBit();
            }
            case BUCKET_3_CONTROL_BITS -> {
                bitBuffer.writeTrueBit();
                bitBuffer.writeTrueBit();
            }
            default -> throw new IllegalArgumentException("unknown controlBits");
        }
    }

    public static List<Integer> decode(BitStream bitStream) {
        ArrayList<Integer> integers = new ArrayList<>();

        String controlBits;
        int lastInteger = -1;

        while (bitStream.hasNNext(AMT_CONTROL_BITS)) {
            controlBits = bitStream.getNBits(AMT_CONTROL_BITS);

            if (SAME_VALUE_ENCODING.equals(controlBits)) {
                if (lastInteger == -1) {
                    throw new IllegalStateException("BucketEncoding:decode: \"Error - first value cannot have control bit '00'\"");
                }
                integers.add(lastInteger);
            } else {
                int bitsInBucket = controlBitsToLength(controlBits);
                if (bitStream.hasNNext(bitsInBucket)) {
                    lastInteger = BitUtil.bits2Int(bitStream.getNBits(bitsInBucket));
                    integers.add(lastInteger);
                } else {
                    //this indicates end of stream, and remaining bits of stream are without significance
                    break;
                }
            }
        }
        return integers;
    }

    private static int controlBitsToLength(String controlBits) {
        switch (controlBits) {
            case BUCKET_1_CONTROL_BITS -> {
                return BUCKET_1_BIT_SIZE;
            }
            case BUCKET_2_CONTROL_BITS -> {
                return BUCKET_2_BIT_SIZE;
            }
            case BUCKET_3_CONTROL_BITS -> {
                return BUCKET_3_BIT_SIZE;
            }
            default ->
                throw new IllegalArgumentException("unknown controlBits");
        }
    }
}
