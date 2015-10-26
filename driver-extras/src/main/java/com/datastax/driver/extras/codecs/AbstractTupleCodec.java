/*
 *      Copyright (C) 2012-2015 DataStax Inc.
 *
 *   Licensed under the Apache License, Version 2.0 (the "License");
 *   you may not use this file except in compliance with the License.
 *   You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *   Unless required by applicable law or agreed to in writing, software
 *   distributed under the License is distributed on an "AS IS" BASIS,
 *   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *   See the License for the specific language governing permissions and
 *   limitations under the License.
 */
package com.datastax.driver.extras.codecs;

import java.nio.ByteBuffer;

import com.google.common.reflect.TypeToken;

import com.datastax.driver.core.*;
import com.datastax.driver.core.exceptions.InvalidTypeException;

/**
 * This codec maps a CQL tuple to a a Java object.
 * Can serve as a base class for codecs dealing with
 * direct tuple-to-Pojo mappings.
 */
public abstract class AbstractTupleCodec<T> extends TypeCodec<T> {

    protected final TupleType definition;

    protected AbstractTupleCodec(TupleType cqlType, Class<T> javaClass) {
        super(cqlType, javaClass);
        this.definition = cqlType;
    }

    protected AbstractTupleCodec(TupleType cqlType, TypeToken<T> javaType) {
        super(cqlType, javaType);
        this.definition = cqlType;
    }

    @Override
    public ByteBuffer serialize(T value, ProtocolVersion protocolVersion) {
        if (value == null)
            return null;
        int size = 0;
        int length = definition.getComponentTypes().size();
        ByteBuffer[] elements = new ByteBuffer[length];
        for (int i = 0; i < length; i++) {
            elements[i] = serializeElement(value, i, protocolVersion);
            size += 4 + (elements[i] == null ? 0 : elements[i].remaining());
        }
        ByteBuffer result = ByteBuffer.allocate(size);
        for (ByteBuffer bb : elements) {
            if (bb == null) {
                result.putInt(-1);
            } else {
                result.putInt(bb.remaining());
                result.put(bb.duplicate());
            }
        }
        return (ByteBuffer)result.flip();
    }

    @Override
    public T deserialize(ByteBuffer bytes, ProtocolVersion protocolVersion) {
        if (bytes == null)
            return null;
        // empty byte buffers will result in empty TupleValues
        ByteBuffer input = bytes.duplicate();
        T value = null;
        int i = 0;
        while (input.hasRemaining() && i < definition.getComponentTypes().size()) {
            int n = input.getInt();
            ByteBuffer element = CodecUtils.readBytes(input, n);
            value = deserializeElement(element, value, i++, protocolVersion);
        }
        return value;
    }

    @Override
    public String format(T value) {
        if (value == null)
            return "NULL";
        StringBuilder sb = new StringBuilder("(");
        int length = definition.getComponentTypes().size();
        for (int i = 0; i < length; i++) {
            if (i > 0)
                sb.append(",");
            formatElement(sb, value, i);
        }
        sb.append(")");
        return sb.toString();
    }

    @Override
    public T parse(String value) {
        if (value == null || value.isEmpty() || value.equalsIgnoreCase("NULL"))
            return null;

        T v = null;

        int idx = ParseUtils.skipSpaces(value, 0);
        if (value.charAt(idx++) != '(')
            throw new InvalidTypeException(String.format("Cannot parse tuple value from \"%s\", at character %d expecting '(' but got '%c'", value, idx, value.charAt(idx)));

        idx = ParseUtils.skipSpaces(value, idx);

        if (value.charAt(idx) == ')')
            return v;

        int i = 0;
        while (idx < value.length()) {
            int n;
            try {
                n = ParseUtils.skipCQLValue(value, idx);
            } catch (IllegalArgumentException e) {
                throw new InvalidTypeException(String.format("Cannot parse tuple value from \"%s\", invalid CQL value at character %d", value, idx), e);
            }

            String input = value.substring(idx, n);
            v = parseElement(input, v, i);
            idx = n;
            i += 1;

            idx = ParseUtils.skipSpaces(value, idx);
            if (value.charAt(idx) == ')')
                return v;
            if (value.charAt(idx) != ',')
                throw new InvalidTypeException(String.format("Cannot parse tuple value from \"%s\", at character %d expecting ',' but got '%c'", value, idx, value.charAt(idx)));
            ++idx; // skip ','

            idx = ParseUtils.skipSpaces(value, idx);
        }
        throw new InvalidTypeException(String.format("Malformed tuple value \"%s\", missing closing ')'", value));
    }

    /**
     * Serialize the {@code index}th element of {@code value}.
     * @param value The value to read from.
     * @param index The element index.
     * @param protocolVersion The protocol version to use.
     * @return The serialized element.
     */
    protected abstract ByteBuffer serializeElement(T value, int index, ProtocolVersion protocolVersion);

    /**
     * Read and set the {@code index}th element of {@code value} from {@code input}.
     * @param input The ByteBuffer to read from.
     * @param value The value to write to.
     * @param index The element index.
     * @param protocolVersion The protocol version to use.
     * @return The value.
     */
    protected abstract T deserializeElement(ByteBuffer input, T value, int index, ProtocolVersion protocolVersion);

    /**
     * Format the {@code index}th element of {@code value} to {@code output}.
     *
     * @param output The StringBuilder to write to.
     * @param value The value to read from.
     * @param index The element index.
     */
    protected abstract void formatElement(StringBuilder output, T value, int index);

    /**
     * Parse and set the {@code index}th element of {@code value} from {@code input}.
     *
     * @param input The String to read from.
     * @param value The value to write to.
     * @param index The element index.
     * @return The value.
     */
    protected abstract T parseElement(String input, T value, int index);

}

