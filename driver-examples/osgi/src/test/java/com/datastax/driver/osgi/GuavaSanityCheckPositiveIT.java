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
package com.datastax.driver.osgi;

import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.LoggerContext;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.Appender;
import ch.qos.logback.core.AppenderBase;
import org.ops4j.pax.exam.Configuration;
import org.ops4j.pax.exam.Option;
import org.ops4j.pax.exam.testng.listener.PaxExam;
import org.slf4j.LoggerFactory;
import org.testng.annotations.Listeners;
import org.testng.annotations.Test;

import static org.ops4j.pax.exam.CoreOptions.options;
import static org.testng.Assert.assertFalse;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.SanityChecks;

import static com.datastax.driver.osgi.BundleOptions.defaultOptions;
import static com.datastax.driver.osgi.BundleOptions.driverBundle;
import static com.datastax.driver.osgi.BundleOptions.guavaBundle;
import static com.datastax.driver.osgi.BundleOptions.nettyBundles;


@Listeners({PaxExam.class})
@Test(groups="short")
public class GuavaSanityCheckPositiveIT {

    @Configuration
    public Option[] guava16_0_1Config() {
        return options(
            driverBundle(),
            nettyBundles(),
            guavaBundle(),
            defaultOptions()
        );
    }

    @Configuration
    public Option[] guava17Config() {
        return options(
            driverBundle(),
            nettyBundles(),
            guavaBundle().version("17.0"),
            defaultOptions()
        );
    }

    @Configuration
    public Option[] guava18Config() {
        return options(
            driverBundle(),
            nettyBundles(),
            guavaBundle().version("18.0"),
            defaultOptions()
        );
    }

    /**
     * <p>
     * Validates that the driver is able to detect that the guava library in the classpath is
     * greater than or equal to version 16.01 and does not log an error in this case.
     * </p>
     *
     * The following configurations are tried (defined via methods with the @Configuration annotation):
     * <ol>
     *   <li>With Guava 16.0.1</li>
     *   <li>With Guava 17.0</li>
     *   <li>With Guava 18.0</li>
     * </ol>
     *
     * @test_category packaging
     * @expected_result A message is not logged at error level.
     * @jira_ticket JAVA-961
     * @since 3.0.0-beta1
     */
    public void should_not_log_guava_version_error_if_gte_16_0_1() {
        LoggerContext lc = (LoggerContext) LoggerFactory.getILoggerFactory();
        Logger logger = (Logger)LoggerFactory.getLogger(SanityChecks.class);

        final StringBuilder events = new StringBuilder();
        Appender<ILoggingEvent> appender = new AppenderBase<ILoggingEvent>() {
            @Override
            protected void append(ILoggingEvent eventObject) {
                events.append(eventObject.toString());
            }
        };
        appender.setContext(lc);
        appender.start();
        logger.addAppender(appender);

        Cluster.builder();
        appender.stop();
        assertFalse(events.toString().contains("Detected Guava issue #1635"));
    }
}
