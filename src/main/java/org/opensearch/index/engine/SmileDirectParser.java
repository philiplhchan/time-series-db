/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.index.engine;

import org.opensearch.core.common.bytes.BytesReference;
import org.opensearch.tsdb.core.model.ByteLabels;
import org.opensearch.tsdb.core.model.Labels;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;

/**
 * Direct SMILE parsing without XContentParser overhead.
 *
 * SMILE format reference:
 * - Header: 0x3A 0x29 0x0A + version byte (4 bytes total)
 * - Object start: 0xFA
 * - Object end: 0xFB
 * - Field names: 0x80-0xBF (short) or 0xF4+ (long)
 * - Strings: 0x40-0x5F (tiny), 0x60-0x7F (short), 0xE0-0xE3 (long)
 * - Integers: 0xC0-0xDF (small), 0x24-0x27 (VInt)
 * - Floats: 0x28 (32-bit), 0x29 (64-bit)
 */
public class SmileDirectParser {

    private static final String LABELS_SEPARATOR = " ";

    // SMILE constants
    private static final byte HEADER_0 = 0x3A;  // ':'
    private static final byte HEADER_1 = 0x29;  // ')'
    private static final byte HEADER_2 = 0x0A;  // '\n'
    // Header byte 4 is version/features byte - we skip it
    private static final int HEADER_SIZE = 4;  // 3 byte signature + 1 byte version/features
    private static final byte TOKEN_START_OBJECT = (byte) 0xFA;
    private static final byte TOKEN_END_OBJECT = (byte) 0xFB;
    private static final byte TOKEN_NULL = 0x21;

    // Field name constants
    private static final String FIELD_LABELS = "labels";
    private static final String FIELD_TIMESTAMP = "timestamp";
    private static final String FIELD_VALUE = "value";
    private static final String FIELD_REFERENCE = "reference";

    private SmileDirectParser() {
        // Utility class
    }

    /**
     * Parse SMILE-encoded bytes directly into TSDBDocument.
     * Avoids XContentParser allocation overhead.
     */
    public static TSDBDocument parse(BytesReference source) {
        byte[] bytes = BytesReference.toBytes(source);
        return parse(bytes, 0, bytes.length);
    }

    /**
     * Parse SMILE-encoded bytes directly into TSDBDocument.
     *
     * @param bytes the byte array containing SMILE data
     * @param offset starting offset in the array
     * @param length number of bytes to parse
     * @return parsed TSDBDocument
     */
    public static TSDBDocument parse(byte[] bytes, int offset, int length) {
        int pos = offset;

        // Validate SMILE header (3-byte signature)
        if (bytes[pos] != HEADER_0 || bytes[pos + 1] != HEADER_1 || bytes[pos + 2] != HEADER_2) {
            throw new IllegalArgumentException("Invalid SMILE header");
        }
        // Skip full header (3 byte signature + 1 byte version/features)
        pos += HEADER_SIZE;

        // Expect object start
        if (bytes[pos] != TOKEN_START_OBJECT) {
            throw new IllegalArgumentException("Expected object start token, got: 0x" + Integer.toHexString(bytes[pos] & 0xFF));
        }
        pos++;

        String rawLabelsString = null;
        long timestamp = 0;
        double value = 0;
        Long reference = null;

        // Parse fields until object end
        while (pos < offset + length && bytes[pos] != TOKEN_END_OBJECT) {
            // Read field name
            ParseResult<String> fieldResult = readFieldName(bytes, pos);
            String fieldName = fieldResult.value;
            pos = fieldResult.newPos;

            // Read value based on field name
            switch (fieldName) {
                case FIELD_LABELS:
                    ParseResult<String> strResult = readString(bytes, pos);
                    rawLabelsString = strResult.value;
                    pos = strResult.newPos;
                    break;

                case FIELD_TIMESTAMP:
                    ParseResult<Long> longResult = readLong(bytes, pos);
                    timestamp = longResult.value;
                    pos = longResult.newPos;
                    break;

                case FIELD_VALUE:
                    ParseResult<Double> doubleResult = readDouble(bytes, pos);
                    value = doubleResult.value;
                    pos = doubleResult.newPos;
                    break;

                case FIELD_REFERENCE:
                    if (bytes[pos] == TOKEN_NULL) {
                        pos++;
                    } else {
                        ParseResult<Long> refResult = readLong(bytes, pos);
                        reference = refResult.value;
                        pos = refResult.newPos;
                    }
                    break;

                default:
                    // Skip unknown field value
                    pos = skipValue(bytes, pos);
            }
        }

        Labels labels = null;
        if (rawLabelsString != null && !rawLabelsString.isEmpty()) {
            labels = ByteLabels.fromStrings(rawLabelsString.split(LABELS_SEPARATOR, -1));
        }

        return new TSDBDocument(labels, timestamp, value, reference, rawLabelsString);
    }

    /**
     * Result holder for parsing operations.
     */
    private static class ParseResult<T> {
        final T value;
        final int newPos;

        ParseResult(T value, int newPos) {
            this.value = value;
            this.newPos = newPos;
        }
    }

    private static ParseResult<String> readFieldName(byte[] bytes, int pos) {
        byte token = bytes[pos];

        if ((token & 0xFC) == 0x80) {
            // Short shared key name reference (0x80-0x83) - not typically used
            throw new UnsupportedOperationException("Shared key references not supported");
        }

        if ((token & 0xC0) == 0x80) {
            // Short ASCII key name (0x80-0xBF): length = (token & 0x3F) + 1
            int len = (token & 0x3F) + 1;
            String name = new String(bytes, pos + 1, len, StandardCharsets.UTF_8);
            return new ParseResult<>(name, pos + 1 + len);
        }

        if (token == (byte) 0xF4) {
            // Long ASCII key name: next byte is length - 2
            int len = (bytes[pos + 1] & 0xFF) + 2;
            String name = new String(bytes, pos + 2, len, StandardCharsets.UTF_8);
            return new ParseResult<>(name, pos + 2 + len);
        }

        throw new IllegalArgumentException("Unsupported field name token: " + Integer.toHexString(token & 0xFF));
    }

    private static ParseResult<String> readString(byte[] bytes, int pos) {
        byte token = bytes[pos];

        if (token == TOKEN_NULL) {
            return new ParseResult<>(null, pos + 1);
        }

        if ((token & 0xE0) == 0x40) {
            // Tiny ASCII (0x40-0x5F): length = (token & 0x1F) + 1
            int len = (token & 0x1F) + 1;
            String value = new String(bytes, pos + 1, len, StandardCharsets.UTF_8);
            return new ParseResult<>(value, pos + 1 + len);
        }

        if ((token & 0xE0) == 0x60) {
            // Short ASCII (0x60-0x7F): length = (token & 0x1F) + 33
            int len = (token & 0x1F) + 33;
            String value = new String(bytes, pos + 1, len, StandardCharsets.UTF_8);
            return new ParseResult<>(value, pos + 1 + len);
        }

        if (token == (byte) 0xE0) {
            // Long unicode (7-bit encoded): next bytes are VInt length, then 7-bit encoded data
            ParseResult<Integer> vint = readVInt(bytes, pos + 1);
            int encodedLen = vint.value;
            // Decode 7-bit encoded string
            String value = decode7BitString(bytes, vint.newPos, encodedLen);
            // Calculate actual bytes consumed (7 bits per character, packed)
            int bytesConsumed = (encodedLen * 7 + 7) / 8;
            return new ParseResult<>(value, vint.newPos + bytesConsumed);
        }

        if (token == (byte) 0xE4) {
            // Long ASCII: next 1-5 bytes are VInt length
            ParseResult<Integer> vint = readVInt(bytes, pos + 1);
            int len = vint.value + 2;
            String value = new String(bytes, vint.newPos, len, StandardCharsets.UTF_8);
            return new ParseResult<>(value, vint.newPos + len);
        }

        throw new IllegalArgumentException("Unsupported string token: " + Integer.toHexString(token & 0xFF));
    }

    private static String decode7BitString(byte[] bytes, int offset, int charCount) {
        // 7-bit encoding packs characters without the high bit
        StringBuilder sb = new StringBuilder(charCount);
        int bitOffset = 0;
        int byteIndex = offset;

        for (int i = 0; i < charCount; i++) {
            int bitsFromFirstByte = 8 - bitOffset;
            int charValue;

            if (bitsFromFirstByte >= 7) {
                // All 7 bits from current byte
                charValue = (bytes[byteIndex] >> (bitsFromFirstByte - 7)) & 0x7F;
                bitOffset += 7;
                if (bitOffset >= 8) {
                    bitOffset -= 8;
                    byteIndex++;
                }
            } else {
                // Need bits from two bytes
                int bitsFromSecondByte = 7 - bitsFromFirstByte;
                charValue = (bytes[byteIndex] & ((1 << bitsFromFirstByte) - 1)) << bitsFromSecondByte;
                byteIndex++;
                charValue |= (bytes[byteIndex] >> (8 - bitsFromSecondByte)) & ((1 << bitsFromSecondByte) - 1);
                bitOffset = bitsFromSecondByte;
            }

            sb.append((char) charValue);
        }

        return sb.toString();
    }

    private static ParseResult<Long> readLong(byte[] bytes, int pos) {
        byte token = bytes[pos];

        if ((token & 0xE0) == 0xC0) {
            // Small int (0xC0-0xDF): value = (token & 0x1F) - 16
            return new ParseResult<>((long) ((token & 0x1F) - 16), pos + 1);
        }

        if (token == 0x24) {
            // VInt (32-bit zigzag)
            ParseResult<Integer> vint = readVInt(bytes, pos + 1);
            return new ParseResult<>((long) zigzagDecode(vint.value), vint.newPos);
        }

        if (token == 0x25) {
            // VLong (64-bit zigzag)
            ParseResult<Long> vlong = readVLong(bytes, pos + 1);
            return new ParseResult<>(zigzagDecodeLong(vlong.value), vlong.newPos);
        }

        if (token == 0x26) {
            // 64-bit raw long (big-endian)
            long value = ByteBuffer.wrap(bytes, pos + 1, 8).order(ByteOrder.BIG_ENDIAN).getLong();
            return new ParseResult<>(value, pos + 9);
        }

        throw new IllegalArgumentException("Unsupported long token: " + Integer.toHexString(token & 0xFF));
    }

    private static ParseResult<Double> readDouble(byte[] bytes, int pos) {
        byte token = bytes[pos];

        if (token == 0x28) {
            // 32-bit float
            float f = ByteBuffer.wrap(bytes, pos + 1, 4).order(ByteOrder.BIG_ENDIAN).getFloat();
            return new ParseResult<>((double) f, pos + 5);
        }

        if (token == 0x29) {
            // 64-bit double
            double d = ByteBuffer.wrap(bytes, pos + 1, 8).order(ByteOrder.BIG_ENDIAN).getDouble();
            return new ParseResult<>(d, pos + 9);
        }

        // Could also be an int representation of a whole number
        if ((token & 0xE0) == 0xC0 || token == 0x24 || token == 0x25 || token == 0x26) {
            ParseResult<Long> longResult = readLong(bytes, pos);
            return new ParseResult<>((double) longResult.value, longResult.newPos);
        }

        throw new IllegalArgumentException("Unsupported double token: " + Integer.toHexString(token & 0xFF));
    }

    private static ParseResult<Integer> readVInt(byte[] bytes, int pos) {
        int value = 0;
        int shift = 0;
        while (true) {
            byte b = bytes[pos++];
            value |= ((b & 0x7F) << shift);
            if ((b & 0x80) == 0) break;
            shift += 7;
        }
        return new ParseResult<>(value, pos);
    }

    private static ParseResult<Long> readVLong(byte[] bytes, int pos) {
        long value = 0;
        int shift = 0;
        while (true) {
            byte b = bytes[pos++];
            value |= ((long) (b & 0x7F) << shift);
            if ((b & 0x80) == 0) break;
            shift += 7;
        }
        return new ParseResult<>(value, pos);
    }

    private static int zigzagDecode(int n) {
        return (n >>> 1) ^ -(n & 1);
    }

    private static long zigzagDecodeLong(long n) {
        return (n >>> 1) ^ -(n & 1);
    }

    private static int skipValue(byte[] bytes, int pos) {
        byte token = bytes[pos];

        if (token == TOKEN_NULL || token == 0x20 || token == 0x21) {
            return pos + 1; // null, false, true
        }
        if ((token & 0xE0) == 0xC0) {
            return pos + 1; // small int
        }
        if ((token & 0xE0) == 0x40) {
            return pos + 1 + ((token & 0x1F) + 1); // tiny string
        }
        if ((token & 0xE0) == 0x60) {
            return pos + 1 + ((token & 0x1F) + 33); // short string
        }
        if (token == 0x28) return pos + 5; // float
        if (token == 0x29) return pos + 9; // double
        if (token == 0x26) return pos + 9; // raw long

        // For complex cases, throw for now
        throw new UnsupportedOperationException("Cannot skip token: " + Integer.toHexString(token & 0xFF));
    }
}
