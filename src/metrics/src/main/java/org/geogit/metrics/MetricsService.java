package org.geogit.metrics;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;

import java.net.URL;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.geogit.api.CommandLocator;
import org.geogit.api.plumbing.ResolveGeogitDir;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.codahale.metrics.ConsoleReporter;
import com.codahale.metrics.Metric;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;
import com.google.common.util.concurrent.AbstractExecutionThreadService;
import com.google.inject.Inject;

public class MetricsService extends AbstractExecutionThreadService {

    private static final Logger LOGGER = LoggerFactory.getLogger(MetricsService.class);

    private final MetricRegistry metricRegistry;

    private final CommandLocator cmdLocator;

    @Inject
    MetricsService(MetricRegistry metricRegistry, CommandLocator cmdLocator) {
        checkNotNull(metricRegistry, "MetricsRegistry");
        checkNotNull(cmdLocator, "CommandLocator");
        this.metricRegistry = metricRegistry;
        this.cmdLocator = cmdLocator;
    }

    @Override
    protected void run() throws Exception {
        LOGGER.debug("Running metrics service");
        while (isRunning()) {
            // do something
        }
    }

    @Override
    protected void startUp() throws Exception {
        LOGGER.trace("Starting up metrics service...");
        final URL geogitDir = cmdLocator.command(ResolveGeogitDir.class).call();
        checkState(geogitDir != null, "Not in a geogit dir");
        checkState("file".equalsIgnoreCase(geogitDir.getProtocol()),
                "Not in a file based geogit directory");

    }

    @Override
    protected void shutDown() throws Exception {
        ConsoleReporter reporter = ConsoleReporter.forRegistry(metricRegistry)
                .convertDurationsTo(TimeUnit.MILLISECONDS).outputTo(System.err).build();
        reporter.report();
//
//        Map<String, Metric> metrics = metricRegistry.getMetrics();
//        for (Map.Entry<String, Metric> e : metrics.entrySet()) {
//            String name = e.getKey();
//            Metric metric = e.getValue();
//
//            System.err.print(name);
//            if (metric instanceof Timer) {
//                Timer t = (Timer) metric;
//                System.err.printf("Count: %s, mean rate: %s\n", t.getCount(), t.getMeanRate());
//            }
//        }
    }

}
