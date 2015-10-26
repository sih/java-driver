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

import static java.util.concurrent.TimeUnit.MILLISECONDS;

import org.joda.time.LocalTime;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import static org.assertj.core.api.Assertions.assertThat;

import static com.datastax.driver.core.ParseUtils.quote;

public class LocalTimeCodecTest {

    @DataProvider(name = "LocalTimeCodecTest")
    public Object[][] parseParameters() {
        LocalTime time = LocalTime.parse("13:25:47.123456789");
        String nanosOfDay = quote(Long.toString(MILLISECONDS.toNanos(time.getMillisOfDay())));
        return new Object[][]{
            { nanosOfDay            , LocalTime.parse("13:25:47.123456789") },
            { "'13:25:47'"          , LocalTime.parse("13:25:47") },
            { "'13:25:47.123'"      , LocalTime.parse("13:25:47.123") },
            { "'13:25:47.123456'"   , LocalTime.parse("13:25:47.123456") },
            { "'13:25:47.123456789'", LocalTime.parse("13:25:47.123456789") }
        };
    }

    @Test(groups = "unit", dataProvider = "LocalTimeCodecTest")
    public void should_parse_valid_formats(String input, LocalTime expected) {
        // when
        LocalTime actual = LocalTimeCodec.instance.parse(input);
        // then
        assertThat(actual).isEqualTo(expected);
    }

    @Test(groups = "unit")
    public void should_format_valid_object() {
        // given
        LocalTime input = LocalTime.parse("13:25:47.123");
        // when
        String actual = LocalTimeCodec.instance.format(input);
        // then
        assertThat(actual).isEqualTo("'13:25:47.123'");
    }

}
