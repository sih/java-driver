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
package com.datastax.driver.extras.codecs.date;

import java.nio.ByteBuffer;
import java.text.ParseException;

import com.datastax.driver.core.DataType;
import com.datastax.driver.core.ParseUtils;
import com.datastax.driver.core.ProtocolVersion;
import com.datastax.driver.core.TypeCodec;
import com.datastax.driver.core.exceptions.InvalidTypeException;

public class TimestampLongCodec extends TypeCodec.PrimitiveLongCodec {

    public TimestampLongCodec() {
        super(DataType.timestamp());
    }

    @Override
    public ByteBuffer serializeNoBoxing(long value, ProtocolVersion protocolVersion) {
        ByteBuffer bb = ByteBuffer.allocate(8);
        bb.putLong(0, value);
        return bb;
    }

    @Override
    public long deserializeNoBoxing(ByteBuffer bytes, ProtocolVersion protocolVersion) {
        if (bytes == null || bytes.remaining() == 0)
            return 0;
        if (bytes.remaining() != 8)
            throw new InvalidTypeException("Invalid 64-bits long value, expecting 8 bytes but got " + bytes.remaining());
        return bytes.getLong(bytes.position());
    }

    @Override
    public Long parse(String value) {
        if (value == null || value.isEmpty() || value.equalsIgnoreCase("NULL"))
            return null;

        // single quotes are optional for long literals, mandatory for date patterns
        // strip enclosing single quotes, if any
        if (value.charAt(0) == '\'' && value.charAt(value.length() - 1) == '\'')
            value = ParseUtils.unquote(value);

        if (ParseUtils.isLongLiteral(value)) {
            try {
                return Long.parseLong(value);
            } catch (NumberFormatException e) {
                throw new InvalidTypeException(String.format("Cannot parse timestamp value from \"%s\"", value));
            }
        }

        try {
            return ParseUtils.parseDate(value).getTime();
        } catch (ParseException e) {
            throw new InvalidTypeException(String.format("Cannot parse timestamp value from \"%s\"", value));
        }
    }

    @Override
    public String format(Long value) {
        if (value == null)
            return "NULL";
        return Long.toString(value);
    }

}
