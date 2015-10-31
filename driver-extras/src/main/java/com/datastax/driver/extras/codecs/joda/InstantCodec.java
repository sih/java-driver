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
package com.datastax.driver.extras.codecs.joda;

import java.nio.ByteBuffer;

import static java.lang.Long.parseLong;

import org.joda.time.Instant;
import org.joda.time.format.*;

import com.datastax.driver.core.DataType;
import com.datastax.driver.core.ProtocolVersion;
import com.datastax.driver.core.TypeCodec;
import com.datastax.driver.core.exceptions.InvalidTypeException;

import static com.datastax.driver.core.ParseUtils.isLongLiteral;
import static com.datastax.driver.core.ParseUtils.quote;
import static com.datastax.driver.core.ParseUtils.unquote;

/**
 * {@link TypeCodec} that maps {@link Instant} to CQL <code>timestamp</code>.
 *
 * @see DateTimeCodec
 * @see "https://cassandra.apache.org/doc/cql3/CQL-2.2.html#usingtimestamps"
 */
public class InstantCodec extends TypeCodec<Instant> {

    public static final InstantCodec instance = new InstantCodec();

    /**
     * A {@link DateTimeFormatter} that parses (most) of
     * the ISO formats accepted in CQL.
     */
    private static final DateTimeFormatter FORMATTER = new DateTimeFormatterBuilder()
        .append(ISODateTimeFormat.dateOptionalTimeParser().getParser())
        .appendOptional(
            new DateTimeFormatterBuilder()
            .appendTimeZoneOffset("Z", true, 2, 4)
            .toParser())
        .toFormatter()
        .withZoneUTC();

    private InstantCodec() {
        super(DataType.timestamp(), Instant.class);
    }

    @Override
    public ByteBuffer serialize(Instant value, ProtocolVersion protocolVersion) {
        return value == null ? null : bigint().serializeNoBoxing(value.getMillis(), protocolVersion);
    }

    @Override
    public Instant deserialize(ByteBuffer bytes, ProtocolVersion protocolVersion) {
        if (bytes == null || bytes.remaining() == 0)
            return null;
        long millis = bigint().deserializeNoBoxing(bytes, protocolVersion);
        return new Instant(millis);
    }

    @Override
    public String format(Instant value) {
        if (value == null)
            return "NULL";
        return quote(value.toString());
    }

    @Override
    public Instant parse(String value) {
        if (value == null || value.isEmpty() || value.equalsIgnoreCase("NULL"))
            return null;
        // strip enclosing single quotes, if any
        if (value.charAt(0) == '\'' && value.charAt(value.length() - 1) == '\'')
            value = unquote(value);
        if (isLongLiteral(value)) {
            try {
                return new Instant(parseLong(value));
            } catch (NumberFormatException e) {
                throw new InvalidTypeException(String.format("Cannot parse timestamp value from \"%s\"", value));
            }
        }
        try {
            return new Instant(FORMATTER.parseMillis(value));
        } catch (RuntimeException e) {
            throw new InvalidTypeException(String.format("Cannot parse timestamp value from \"%s\"", value));
        }
    }

}
