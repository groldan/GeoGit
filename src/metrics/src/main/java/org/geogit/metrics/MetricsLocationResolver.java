package org.geogit.metrics;

import java.io.File;
import java.net.URISyntaxException;
import java.net.URL;

import org.geogit.api.CommandLocator;
import org.geogit.api.plumbing.ResolveGeogitDir;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Optional;
import com.google.inject.Inject;

class MetricsLocationResolver {

    private static final Logger LOGGER = LoggerFactory.getLogger(MetricsLocationResolver.class);

    private CommandLocator cmdLocator;

    //private File resolvedMetricsDirectory;

    @Inject
    MetricsLocationResolver(CommandLocator cmdLocator) {
        this.cmdLocator = cmdLocator;
    }

    public synchronized Optional<File> getMetricsDirectory() {
//        if (resolvedMetricsDirectory != null) {
//            return Optional.of(resolvedMetricsDirectory);
//        }

        final URL geogitDir = cmdLocator.command(ResolveGeogitDir.class).call();
        if (geogitDir == null) {
            LOGGER.debug("Not in a geogit dir");
            return Optional.absent();
        }
        if (!"file".equalsIgnoreCase(geogitDir.getProtocol())) {
            LOGGER.debug("Not in a file based geogit directory");
            return Optional.absent();
        }

        File metricsDirectory;
        try {
            metricsDirectory = new File(new File(new File(geogitDir.toURI()), "log"), "metrics");
        } catch (URISyntaxException e) {
            LOGGER.warn("Syntax exception in converting geogit dir ({}) to File", geogitDir, e);
            return Optional.absent();
        }

        if (!metricsDirectory.exists() && !metricsDirectory.mkdirs()) {
            LOGGER.warn("Can't create directory to store metrics {}", metricsDirectory);
            return Optional.absent();
        }
        if (!metricsDirectory.isDirectory()) {
            LOGGER.warn("{} exists and is not a directory!", metricsDirectory);
            return Optional.absent();
        }
        if (!metricsDirectory.canWrite()) {
            LOGGER.warn("Can't write to {}", metricsDirectory);
            return Optional.absent();
        }
        //resolvedMetricsDirectory = metricsDirectory;
        return Optional.of(metricsDirectory);
    }
}
