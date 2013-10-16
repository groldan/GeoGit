/* Copyright (c) 2013 OpenPlans. All rights reserved.
 * This code is licensed under the BSD New License, available at the root
 * application directory.
 */
package org.geogit.metrics;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;

import java.io.File;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.geogit.storage.ConfigDatabase;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.codahale.metrics.Counting;
import com.codahale.metrics.CsvReporter;
import com.codahale.metrics.Metric;
import com.codahale.metrics.MetricFilter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Slf4jReporter;
import com.google.common.base.Optional;
import com.google.common.collect.Maps;
import com.google.common.util.concurrent.AbstractIdleService;
import com.google.inject.Inject;

public class MetricsLoggingService extends AbstractIdleService {

    private static final Logger LOGGER = LoggerFactory.getLogger(MetricsLoggingService.class);

    private static final Logger METRICS_LOGGER = LoggerFactory
            .getLogger("org.geogit.metrics.Metrics");

    private final MetricRegistry metricRegistry;

    /**
     * a {@link LoggingFilter} as an instance variable that's governed by this service's lifecycle
     * methods
     */
    private LoggingFilter loggingFilter;

    private static final class LoggingFilter implements MetricFilter {

        private Map<String, Long> previousCounts = Maps.newHashMap();

        boolean allowAll;

        @Override
        public boolean matches(final String name, final Metric metric) {
            if (metric instanceof Counting) {// Meter, Timer, Counter, Histogram
                // Filter out counters that haven't changed since the last run
                final Counting counter = (Counting) metric;
                if (allowAll) {
                    return counter.getCount() > 0L;
                }
                final long oldCount = Optional.fromNullable(previousCounts.get(name))
                        .or(Long.valueOf(0)).longValue();
                final long newCount = counter.getCount();
                if (newCount == oldCount) {
                    return false;
                }
                previousCounts.put(name, newCount);
            }
            return true;
        }
    };

    private Slf4jReporter loggingReporter;

    private CsvReporter csvReporter;

    private MetricsLocationResolver locationResolver;

    private ConfigDatabase configDb;

    @Inject
    MetricsLoggingService(MetricRegistry metricRegistry, MetricsLocationResolver locationResolver,
            ConfigDatabase configDb) {
        this.configDb = configDb;
        checkNotNull(metricRegistry, "MetricsRegistry");
        checkNotNull(locationResolver, "locationResolver");
        this.metricRegistry = metricRegistry;
        this.locationResolver = locationResolver;
    }

    @Override
    protected void startUp() throws Exception {
        if (!"true".equals(configDb.get("metrics.enabled").orNull())) {
            return;
        }

        LOGGER.trace("Starting up metrics service...");
        this.loggingFilter = new LoggingFilter();

        final Optional<File> metricsDirectory = locationResolver.getMetricsDirectory();
        checkState(metricsDirectory.isPresent(), "Couldn't resolve location of metrics directory");

        loggingReporter = Slf4jReporter.forRegistry(metricRegistry)//
                .outputTo(METRICS_LOGGER)//
                .convertDurationsTo(TimeUnit.MILLISECONDS)//
                .convertRatesTo(TimeUnit.SECONDS)//
                .filter(loggingFilter)//
                .build();
        loggingReporter.start(2, TimeUnit.SECONDS);

        final File metricsTargetDir = metricsDirectory.get();
        csvReporter = CsvReporter.forRegistry(metricRegistry)//
                .filter(loggingFilter)//
                .formatFor(Locale.ENGLISH)//
                .convertDurationsTo(TimeUnit.MILLISECONDS)//
                .convertRatesTo(TimeUnit.SECONDS)//
                .build(metricsTargetDir);
        csvReporter.start(5, TimeUnit.SECONDS);
    }

    @Override
    protected void shutDown() throws Exception {
        if (loggingFilter != null) {
            loggingFilter.allowAll = true;
        }
        if (loggingReporter != null) {
            loggingReporter.report();
            loggingReporter.stop();
            loggingReporter = null;
        }
        if (csvReporter != null) {
            csvReporter.report();
            csvReporter.stop();
            csvReporter = null;
        }
        if (loggingFilter != null) {
            // loggingFilter.previousCounts.clear();
            loggingFilter = null;
        }
    }

}
