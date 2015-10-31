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
package com.datastax.driver.extras.codecs.jdk8;

import java.nio.ByteBuffer;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.time.format.DateTimeParseException;
import java.util.List;

import static java.time.temporal.ChronoField.HOUR_OF_DAY;
import static java.time.temporal.ChronoField.MINUTE_OF_HOUR;
import static java.time.temporal.ChronoField.NANO_OF_SECOND;
import static java.time.temporal.ChronoField.SECOND_OF_MINUTE;

import static com.google.common.base.Preconditions.checkArgument;

import com.datastax.driver.core.*;
import com.datastax.driver.core.exceptions.InvalidTypeException;
import com.datastax.driver.extras.codecs.AbstractTupleCodec;

import static com.datastax.driver.core.ParseUtils.isLongLiteral;
import static com.datastax.driver.core.ParseUtils.quote;

/**
 * <p>
 * {@link TypeCodec} that maps
 * {@link ZonedDateTime} <-> <code>tuple&lt;timestamp,varchar&gt;</code>
 * providing a pattern for maintaining timezone information in
 * Cassandra.
 *
 * <p>
 * Since Cassandra's <code>timestamp</code> type preserves only
 * milliseconds since epoch, any timezone information
 * would normally be lost. By using a
 * <code>tuple&lt;timestamp,varchar&gt;</code> a timezone ID can be
 * persisted in the <code>varchar</code> field such that when the
 * value is deserialized the timezone is
 * preserved.
 */
public class ZonedDateTimeCodec extends AbstractTupleCodec<ZonedDateTime> {

    /**
     * A {@link DateTimeFormatter} that parses (most) of
     * the ISO formats accepted in CQL.
     */
    private static final DateTimeFormatter FORMATTER = new DateTimeFormatterBuilder()
        .parseCaseSensitive()
        .parseStrict()
        .append(DateTimeFormatter.ISO_LOCAL_DATE)
        .optionalStart()
        .appendLiteral('T')
        .appendValue(HOUR_OF_DAY, 2)
        .appendLiteral(':')
        .appendValue(MINUTE_OF_HOUR, 2)
        .optionalEnd()
        .optionalStart()
        .appendLiteral(':')
        .appendValue(SECOND_OF_MINUTE, 2)
        .optionalEnd()
        .optionalStart()
        .appendFraction(NANO_OF_SECOND, 0, 9, true)
        .optionalEnd()
        .optionalStart()
        .appendZoneId()
        .optionalEnd()
        .toFormatter()
        .withZone(ZoneOffset.UTC);

    public ZonedDateTimeCodec(TupleType tupleType) {
        super(tupleType, ZonedDateTime.class);
        List<DataType> types = tupleType.getComponentTypes();
        checkArgument(
            types.size() == 2 && types.get(0).equals(DataType.timestamp()) && types.get(1).equals(DataType.varchar()),
            "Expected tuple<timestamp,varchar>, got %s",
            tupleType);
    }

    @Override
    protected ByteBuffer serializeElement(ZonedDateTime value, int index, ProtocolVersion protocolVersion) {
        if(index == 0) {
            long millis = value.toInstant().toEpochMilli();
            return bigint().serializeNoBoxing(millis, protocolVersion);
        }
        if(index == 1) {
            return varchar().serialize(value.getZone().getId(), protocolVersion);
        }
        throw new IndexOutOfBoundsException("Tuple index out of bounds. "+ index);
    }

    @Override
    protected ZonedDateTime deserializeElement(ByteBuffer input, ZonedDateTime value, int index, ProtocolVersion protocolVersion) {
        if(index == 0) {
            long millis = bigint().deserializeNoBoxing(input, protocolVersion);
            return Instant.ofEpochMilli(millis).atZone(ZoneOffset.UTC);
        }
        if(index == 1) {
            String zoneId = varchar().deserialize(input, protocolVersion);
            return value.withZoneSameInstant(ZoneId.of(zoneId));
        }
        throw new IndexOutOfBoundsException("Tuple index out of bounds. "+ index);
    }

    @Override
    protected void formatElement(StringBuilder output, ZonedDateTime value, int index) {
        if(index == 0) {
            output.append(quote(FORMATTER.format(value)));
            return;
        }
        if(index == 1) {
            output.append(quote(value.getZone().getId()));
            return;
        }
        throw new IndexOutOfBoundsException("Tuple index out of bounds. "+ index);
    }

    @Override
    protected ZonedDateTime parseElement(String input, ZonedDateTime value, int index) {
        if(index == 0) {
            // strip enclosing single quotes, if any
            if (input.charAt(0) == '\'' && input.charAt(input.length() - 1) == '\'')
                input = ParseUtils.unquote(input);
            if (isLongLiteral(input)) {
                try {
                    long millis = Long.parseLong(input);
                    return ZonedDateTime.ofInstant(Instant.ofEpochMilli(millis), ZoneOffset.UTC);
                } catch (NumberFormatException e) {
                    throw new InvalidTypeException(String.format("Cannot parse timestamp value from \"%s\"", input));
                }
            }
            try {
                return ZonedDateTime.from(FORMATTER.parse(input));
            } catch (DateTimeParseException e) {
                throw new InvalidTypeException(String.format("Cannot parse timestamp value from \"%s\"", value));
            }
        }
        if(index == 1) {
            String zoneId = varchar().parse(input);
            return value.withZoneSameInstant(ZoneId.of(zoneId));
        }
        throw new IndexOutOfBoundsException("Tuple index out of bounds. "+ index);
    }
}
