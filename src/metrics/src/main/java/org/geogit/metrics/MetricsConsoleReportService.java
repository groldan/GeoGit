/* Copyright (c) 2013 OpenPlans. All rights reserved.
 * This code is licensed under the BSD New License, available at the root
 * application directory.
 */
package org.geogit.metrics;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;

import java.io.File;
import java.io.FileOutputStream;
import java.io.PrintStream;
import java.util.Locale;
import java.util.concurrent.TimeUnit;

import com.codahale.metrics.ConsoleReporter;
import com.codahale.metrics.Counting;
import com.codahale.metrics.Metric;
import com.codahale.metrics.MetricFilter;
import com.codahale.metrics.MetricRegistry;
import com.google.common.base.Optional;
import com.google.common.util.concurrent.AbstractIdleService;
import com.google.inject.Inject;

/**
 * Service that writes a {@link ConsoleReporter console report} to the
 * {@code .geogit/metrics/metrics_shutdown_report.txt} file on shutdown.
 */
public class MetricsConsoleReportService extends AbstractIdleService {

    private MetricRegistry metricRegistry;

    private MetricsLocationResolver locationResolver;

    private static MetricFilter FILTER_UNUSED = new MetricFilter() {

        @Override
        public boolean matches(String name, Metric metric) {
            if (metric instanceof Counting) {
                return ((Counting) metric).getCount() > 0;
            }
            return true;
        }
    };

    @Inject
    MetricsConsoleReportService(MetricRegistry metricRegistry,
            MetricsLocationResolver locationResolver) {
        checkNotNull(metricRegistry, "MetricsRegistry");
        checkNotNull(locationResolver, "locationResolver");
        this.metricRegistry = metricRegistry;
        this.locationResolver = locationResolver;
    }

    @Override
    protected void startUp() throws Exception {
        Optional<File> metricsDirectory = locationResolver.getMetricsDirectory();
        checkState(metricsDirectory.isPresent(),
                "Metrics directory couldn't be determined, can't start service.");
    }

    @Override
    protected void shutDown() throws Exception {
        final File metricsDir = locationResolver.getMetricsDirectory().orNull();
        if (metricsDir == null) {
            return;
        }
        final File reportFile = new File(metricsDir, "metrics_shutdown_report.txt");
        FileOutputStream fout = new FileOutputStream(reportFile);
        try {
            PrintStream printStream = new PrintStream(fout, true, "UTF-8");
            reportTo(printStream);
        } finally {
            fout.close();
        }
    }

    public void reportTo(final PrintStream output) {
        ConsoleReporter.forRegistry(metricRegistry)//
                .convertDurationsTo(TimeUnit.MILLISECONDS)//
                .convertRatesTo(TimeUnit.SECONDS)//
                .filter(FILTER_UNUSED)//
                .formattedFor(Locale.ENGLISH)//
                .outputTo(output)//
                .build();
    }
}
