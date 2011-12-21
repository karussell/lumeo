/*
 *  Copyright 2011 Peter Karich info@jetsli.de
 * 
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 * 
 *       http://www.apache.org/licenses/LICENSE-2.0
 * 
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */
package de.jetsli.lumeo.util;

import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.NumericUtils;

/**
 * @author Peter Karich, info@jetsli.de
 */
public class LuceneHelper {    

    public static BytesRef newRefFromInt(int value) {
        // TODO reuse bytes
        final BytesRef bytes = new BytesRef();
        NumericUtils.intToPrefixCoded(value, 0, bytes);
        return bytes;
    }

    public static BytesRef newRefFromLong(long value) {
        // TODO reuse bytes
        final BytesRef bytes = new BytesRef();
//        copyLong(bytes, value);
        NumericUtils.longToPrefixCoded(value, 0, bytes);
        return bytes;
    }

    public static BytesRef newRefFromDouble(double value) {
        return newRefFromLong(Double.doubleToLongBits(value));
    }

    /**
     * Copies the given int value and encodes it as 4 byte Big-Endian.
     * <p>
     * NOTE: this method resets the offset to 0, length to 4 and resizes the
     * reference array if needed.
     */
    static void copyInt(BytesRef ref, int value) {
        if (ref.bytes.length < 4)
            ref.bytes = new byte[4];

        ref.bytes[0] = (byte) (value >> 24);
        ref.bytes[1] = (byte) (value >> 16);
        ref.bytes[2] = (byte) (value >> 8);
        ref.bytes[3] = (byte) (value);
        ref.length = 4;
    }

    /**
     * Copies the given int value and encodes it as 4 byte Big-Endian.
     * <p>
     * NOTE: this method resets the offset to 0, length to 4 and resizes the
     * reference array if needed.
     */
    static void copyLong(BytesRef ref, long value) {
        if (ref.bytes.length < 8)
            ref.bytes = new byte[8];

        ref.bytes[0] = (byte) (value >> 56);
        ref.bytes[1] = (byte) (value >> 48);
        ref.bytes[2] = (byte) (value >> 40);
        ref.bytes[3] = (byte) (value >> 32);
        ref.bytes[4] = (byte) (value >> 24);
        ref.bytes[5] = (byte) (value >> 16);
        ref.bytes[6] = (byte) (value >> 8);
        ref.bytes[7] = (byte) (value);
        ref.length = 8;
    }

    /**
     * Converts 4 consecutive bytes from the current offset to an int. Bytes are
     * interpreted as Big-Endian (most significant bit first)
     * <p>
     * NOTE: this method does <b>NOT</b> check the bounds of the referenced array.
     */
    static int asInt(BytesRef b) {
        return asIntInternal(b, b.offset);
    }

    /**
     * Converts 8 consecutive bytes from the current offset to a long. Bytes are
     * interpreted as Big-Endian (most significant bit first)
     * <p>
     * NOTE: this method does <b>NOT</b> check the bounds of the referenced array.
     */
    static long asLong(BytesRef b) {
        return (((long) asIntInternal(b, b.offset) << 32) | asIntInternal(b,
                b.offset + 4) & 0xFFFFFFFFL);
    }

    static int asIntInternal(BytesRef b, int pos) {
        return ((b.bytes[pos++] & 0xFF) << 24) | ((b.bytes[pos++] & 0xFF) << 16)
                | ((b.bytes[pos++] & 0xFF) << 8) | (b.bytes[pos] & 0xFF);
    }        
}
