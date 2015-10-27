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

import java.util.Collection;
import java.util.Collections;

import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.joda.time.DateTimeZone.forID;

import com.datastax.driver.core.CCMBridge;
import com.datastax.driver.core.TupleType;

import static com.datastax.driver.core.DataType.timestamp;
import static com.datastax.driver.core.DataType.varchar;

public class DateTimeCodecTest extends CCMBridge.PerClassSingleNodeCluster {

    @DataProvider(name = "DateTimeCodecTest")
    public Object[][] parseParameters() {
        return new Object[][]{
            { "(1277860847999             ,'+01:00')" , new DateTime("2010-06-30T02:20:47.999+01:00", forID("+01:00")) },
            { "('2010-06-30T01:20Z'       ,'+01:00')" , new DateTime("2010-06-30T02:20:00.000+01:00", forID("+01:00")) },
            { "('2010-06-30T01:20:47Z'    ,'+01:00')" , new DateTime("2010-06-30T02:20:47.000+01:00", forID("+01:00")) },
            { "('2010-06-30T01:20:47.999Z','+01:00')" , new DateTime("2010-06-30T02:20:47.999+01:00", forID("+01:00")) }
        };
    }

    @Test(groups = "short", dataProvider = "DateTimeCodecTest")
    public void should_parse_valid_formats(String input, DateTime expected) {
        // given
        TupleType tupleType = cluster.getMetadata().newTupleType(timestamp(), varchar());
        DateTimeCodec codec = new DateTimeCodec(tupleType);
        // when
        DateTime actual = codec.parse(input);
        // then
        assertThat(actual).isEqualTo(expected);
    }

    @Test(groups = "short")
    public void should_format_valid_object() {
        // given
        TupleType tupleType = cluster.getMetadata().newTupleType(timestamp(), varchar());
        DateTimeCodec codec = new DateTimeCodec(tupleType);
        DateTime input = DateTime.parse("2010-06-30T02:20:47.999+01:00");
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
