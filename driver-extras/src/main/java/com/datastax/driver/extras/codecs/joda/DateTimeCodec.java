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
import java.time.format.DateTimeParseException;
import java.util.List;

import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.format.DateTimeFormatter;
import org.joda.time.format.DateTimeFormatterBuilder;
import org.joda.time.format.ISODateTimeFormat;

import static com.google.common.base.Preconditions.checkArgument;
import static org.joda.time.DateTimeZone.UTC;

import com.datastax.driver.core.*;
import com.datastax.driver.core.exceptions.InvalidTypeException;
import com.datastax.driver.extras.codecs.AbstractTupleCodec;

import static com.datastax.driver.core.ParseUtils.isLongLiteral;
import static com.datastax.driver.core.ParseUtils.quote;

/**
 * <p>
 * {@link TypeCodec} that maps
 * {@link DateTime} <-> <code>tuple&lt;timestamp,varchar&gt;</code>
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
public class DateTimeCodec extends AbstractTupleCodec<DateTime> {

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

    public DateTimeCodec(TupleType tupleType) {
        super(tupleType, DateTime.class);
        List<DataType> types = tupleType.getComponentTypes();
        checkArgument(
            types.size() == 2 && types.get(0).equals(DataType.timestamp()) && types.get(1).equals(DataType.varchar()),
            "Expected tuple<timestamp,varchar>, got %s",
            tupleType);
    }

    @Override
    protected ByteBuffer serializeElement(DateTime value, int index, ProtocolVersion protocolVersion) {
        if(index == 0) {
            long millis = value.getMillis();
            return bigint().serializeNoBoxing(millis, protocolVersion);
        }
        if(index == 1) {
            return varchar().serialize(value.getZone().getID(), protocolVersion);
        }
        throw new IndexOutOfBoundsException("Tuple index out of bounds. "+ index);
    }

    @Override
    protected DateTime deserializeElement(ByteBuffer input, DateTime value, int index, ProtocolVersion protocolVersion) {
        if(index == 0) {
            long millis = bigint().deserializeNoBoxing(input, protocolVersion);
            return new DateTime(millis);
        }
        if(index == 1) {
            String zoneId = varchar().deserialize(input, protocolVersion);
            return value.withZone(DateTimeZone.forID(zoneId));
        }
        throw new IndexOutOfBoundsException("Tuple index out of bounds. "+ index);
    }

    @Override
    protected void formatElement(StringBuilder output, DateTime value, int index) {
        if(index == 0) {
            output.append(quote(value.withZone(UTC).toString()));
            return;
        }
        if(index == 1) {
            output.append(quote(value.getZone().getID()));
            return;
        }
        throw new IndexOutOfBoundsException("Tuple index out of bounds. "+ index);
    }

    @Override
    protected DateTime parseElement(String input, DateTime value, int index) {
        if(index == 0) {
            // strip enclosing single quotes, if any
            if (input.charAt(0) == '\'' && input.charAt(input.length() - 1) == '\'')
                input = ParseUtils.unquote(input);
            if (isLongLiteral(input)) {
                try {
                    long millis = Long.parseLong(input);
                    return new DateTime(millis);
                } catch (NumberFormatException e) {
                    throw new InvalidTypeException(String.format("Cannot parse timestamp value from \"%s\"", input));
                }
            }
            try {
                return FORMATTER.parseDateTime(input);
            } catch (DateTimeParseException e) {
                throw new InvalidTypeException(String.format("Cannot parse timestamp value from \"%s\"", value));
            }
        }
        if(index == 1) {
            String zoneId = varchar().parse(input);
            return value.withZone(DateTimeZone.forID(zoneId));
        }
        throw new IndexOutOfBoundsException("Tuple index out of bounds. "+ index);
    }

}
