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
import java.time.chrono.IsoChronology;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.time.format.DateTimeParseException;
import java.time.format.ResolverStyle;
import java.time.temporal.TemporalQueries;
import java.util.Date;

import static java.lang.Long.parseLong;
import static java.time.Instant.ofEpochMilli;
import static java.time.temporal.ChronoField.HOUR_OF_DAY;
import static java.time.temporal.ChronoField.MINUTE_OF_HOUR;
import static java.time.temporal.ChronoField.NANO_OF_SECOND;
import static java.time.temporal.ChronoField.SECOND_OF_MINUTE;

import com.datastax.driver.core.DataType;
import com.datastax.driver.core.ProtocolVersion;
import com.datastax.driver.core.TypeCodec;
import com.datastax.driver.core.exceptions.InvalidTypeException;

import static com.datastax.driver.core.ParseUtils.isLongLiteral;
import static com.datastax.driver.core.ParseUtils.quote;
import static com.datastax.driver.core.ParseUtils.unquote;

/**
 * <p>
 * {@link TypeCodec} that maps {@link Instant} <-> {@link Date}
 * allowing the setting and retrieval of <code>timestamp</code>
 * columns as {@link Instant} instances.
 * </p>
 *
 * <p>
 * Since C* <code>timestamp</code> columns do not preserve timezones
 * any attached timezone information will be lost.
 * </p>
 *
 * @see ZonedDateTimeCodec
 * @see "https://cassandra.apache.org/doc/cql3/CQL-2.2.html#usingtimestamps"
 */
public class InstantCodec extends TypeCodec<Instant> {

    public static final InstantCodec instance = new InstantCodec();

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

    private InstantCodec() {
        super(DataType.timestamp(), Instant.class);
    }

    @Override
    public ByteBuffer serialize(Instant value, ProtocolVersion protocolVersion) {
        return value == null ? null : bigint().serializeNoBoxing(value.toEpochMilli(), protocolVersion);
    }

    @Override
    public Instant deserialize(ByteBuffer bytes, ProtocolVersion protocolVersion) {
        return bytes == null || bytes.remaining() == 0 ? null : ofEpochMilli(bigint().deserializeNoBoxing(bytes, protocolVersion));
    }

    @Override
    public String format(Instant value) {
        if (value == null)
            return "NULL";
        return quote(FORMATTER.format(value));
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
                return ofEpochMilli(parseLong(value));
            } catch (NumberFormatException e) {
                throw new InvalidTypeException(String.format("Cannot parse timestamp value from \"%s\"", value));
            }
        }
        try {
            return Instant.from(FORMATTER.parse(value));
        } catch (DateTimeParseException e) {
            throw new InvalidTypeException(String.format("Cannot parse timestamp value from \"%s\"", value));
        }
    }

}
