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

import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.Collection;
import java.util.Collections;

import static java.time.Instant.parse;
import static java.time.ZonedDateTime.ofInstant;

import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import static org.assertj.core.api.Assertions.assertThat;

import com.datastax.driver.core.CCMBridge;
import com.datastax.driver.core.TupleType;

import static com.datastax.driver.core.DataType.timestamp;
import static com.datastax.driver.core.DataType.varchar;

public class ZonedDateTimeCodecTest extends CCMBridge.PerClassSingleNodeCluster {

    @DataProvider(name = "ZonedDateTimeCodecTest")
    public Object[][] parseParameters() {
        return new Object[][]{
            { "(1277860847999             ,'+01:00')" , ofInstant(parse("2010-06-30T01:20:47.999Z"), ZoneOffset.of("+01:00")) },
            { "('2010-06-30T01:20Z'       ,'+01:00')" , ofInstant(parse("2010-06-30T01:20:00.000Z"), ZoneOffset.of("+01:00")) },
            { "('2010-06-30T01:20:47Z'    ,'+01:00')" , ofInstant(parse("2010-06-30T01:20:47.000Z"), ZoneOffset.of("+01:00")) },
            { "('2010-06-30T01:20:47.999Z','+01:00')" , ofInstant(parse("2010-06-30T01:20:47.999Z"), ZoneOffset.of("+01:00")) }
        };
    }

    @Test(groups = "short", dataProvider = "ZonedDateTimeCodecTest")
    public void should_parse_valid_formats(String input, ZonedDateTime expected) {
        // given
        TupleType tupleType = cluster.getMetadata().newTupleType(timestamp(), varchar());
        ZonedDateTimeCodec codec = new ZonedDateTimeCodec(tupleType);
        // when
        ZonedDateTime actual = codec.parse(input);
        // then
        assertThat(actual).isEqualTo(expected);
    }

    @Test(groups = "short")
    public void should_format_valid_object() {
        // given
        TupleType tupleType = cluster.getMetadata().newTupleType(timestamp(), varchar());
        ZonedDateTimeCodec codec = new ZonedDateTimeCodec(tupleType);
        ZonedDateTime input = ZonedDateTime.parse("2010-06-30T02:20:47.999+01:00");
        // when
        String actual = codec.format(input);
        // then
        assertThat(actual).isEqualTo("('2010-06-30T01:20:47.999Z','+01:00')");
    }

    @Override
    protected Collection<String> getTableDefinitions() {
        return Collections.emptyList();
    }
}
