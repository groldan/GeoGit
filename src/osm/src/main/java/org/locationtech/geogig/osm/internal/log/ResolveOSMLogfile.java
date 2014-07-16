/* Copyright (c) 2013 OpenPlans. All rights reserved.
 * This code is licensed under the BSD New License, available at the root
 * application directory.
 */
package org.locationtech.geogig.osm.internal.log;

import java.io.File;
import java.io.IOException;
import java.net.URISyntaxException;
import java.net.URL;

import org.locationtech.geogig.api.AbstractGeoGigOp;
import org.locationtech.geogig.api.Platform;
import org.locationtech.geogig.api.plumbing.ResolveGeogigDir;

import com.google.common.base.Throwables;
import com.google.common.io.Files;

/**
 * Resolves the location of the {@code osm} log file directory relative to the
 * {@link Platform#pwd() current directory}.
 * <p>
 * If the osm directory of the osm log file are not found, but we are within a geogig repo, they
 * will be created as needed.
 * 
 */
public class ResolveOSMLogfile extends AbstractGeoGigOp<URL> {

    @Override
    protected URL _call() {
        final URL geogigDirUrl = command(ResolveGeogigDir.class).call().get();
        File repoDir;
        try {
            repoDir = new File(geogigDirUrl.toURI());
        } catch (URISyntaxException e) {
            throw Throwables.propagate(e);
        }
        File osmLogFile = new File(new File(repoDir, "osm"), "log");
        URL url;
        try {
            Files.createParentDirs(osmLogFile);
            if (!osmLogFile.exists()) {
                Files.touch(osmLogFile);
            }
            url = osmLogFile.toURI().toURL();
        } catch (IOException e) {
            throw Throwables.propagate(e);
        }
        return url;

    }

}
