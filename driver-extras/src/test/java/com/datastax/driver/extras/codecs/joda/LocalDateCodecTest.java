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

import org.joda.time.Days;
import org.joda.time.LocalDate;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.joda.time.Days.daysBetween;

import com.datastax.driver.core.CodecUtils;

public class LocalDateCodecTest {

    private static final LocalDate EPOCH = new LocalDate(0);

    @DataProvider(name = "LocalDateCodecTest")
    public Object[][] parseParameters() {
        LocalDate ld = LocalDate.parse("2010-06-30");
        Days days = daysBetween(EPOCH, ld);
        String raw = Long.toString(CodecUtils.fromDaysSinceEpoch(days.getDays()));
        return new Object[][]{
            { raw, ld },
            { "'2010-06-30'", ld }
        };
    }

    @Test(groups = "unit", dataProvider = "LocalDateCodecTest")
    public void should_parse_valid_formats(String input, LocalDate expected) {
        // when
        LocalDate actual = LocalDateCodec.instance.parse(input);
        // then
        assertThat(actual).isEqualTo(expected);
    }

    @Test(groups = "unit")
    public void should_format_valid_object() {
        // given
        LocalDate input = LocalDate.parse("2010-06-30");
        // when
        String actual = LocalDateCodec.instance.format(input);
        // then
        assertThat(actual).isEqualTo("'2010-06-30'");
    }

}
