/* Copyright (c) 2013 OpenPlans. All rights reserved.
 * This code is licensed under the BSD New License, available at the root
 * application directory.
 */
package org.geogit.metrics;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;

import java.io.File;
import java.util.Locale;
import java.util.concurrent.TimeUnit;

import org.geogit.storage.ConfigDatabase;

import com.codahale.metrics.CsvReporter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.jvm.GarbageCollectorMetricSet;
import com.codahale.metrics.jvm.MemoryUsageGaugeSet;
import com.codahale.metrics.jvm.ThreadStatesGaugeSet;
import com.google.common.base.Optional;
import com.google.common.util.concurrent.AbstractIdleService;
import com.google.inject.Inject;

public class JVMMetricsLoggingService extends AbstractIdleService {

    private static final MetricRegistry JVMmetricRegistry = new MetricRegistry();
    static {
        JVMmetricRegistry.registerAll(new MemoryUsageGaugeSet());
        JVMmetricRegistry.registerAll(new GarbageCollectorMetricSet());
        JVMmetricRegistry.registerAll(new ThreadStatesGaugeSet());
    }

    private CsvReporter csvReporter;

    private MetricsLocationResolver locationResolver;

    private ConfigDatabase configDb;

    @Inject
    JVMMetricsLoggingService(MetricRegistry metricRegistry,
            MetricsLocationResolver locationResolver, ConfigDatabase configDb) {
        checkNotNull(metricRegistry, "MetricsRegistry");
        checkNotNull(locationResolver, "locationResolver");
        this.locationResolver = locationResolver;
        this.configDb = configDb;
    }

    @Override
    protected void startUp() throws Exception {
        if (!"true".equals(configDb.get("metrics.enabled").orNull())) {
            return;
        }
        final Optional<File> metricsDirectory = locationResolver.getMetricsDirectory();
        checkState(metricsDirectory.isPresent(), "Couldn't resolve location of metrics directory");

        final File jvmMetricsTargetDir = new File(metricsDirectory.get(), "jvm");
        checkState((jvmMetricsTargetDir.exists() && jvmMetricsTargetDir.isDirectory() && jvmMetricsTargetDir
                .canWrite()) || (jvmMetricsTargetDir.mkdirs()));

        csvReporter = CsvReporter.forRegistry(JVMmetricRegistry)//
                .formatFor(Locale.ENGLISH)//
                .convertDurationsTo(TimeUnit.MILLISECONDS)//
                .convertRatesTo(TimeUnit.SECONDS)//
                .build(jvmMetricsTargetDir);
        csvReporter.start(5, TimeUnit.SECONDS);
    }

    @Override
    protected void shutDown() throws Exception {
        if (csvReporter != null) {
            csvReporter.report();
            csvReporter.stop();
            csvReporter = null;
        }
    }

}
