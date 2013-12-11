/* Copyright (c) 2013 OpenPlans. All rights reserved.
 * This code is licensed under the BSD New License, available at the root
 * application directory.
 */
package org.geogit.osm.internal.log;

import java.io.File;
import java.net.URISyntaxException;
import java.net.URL;

import org.geogit.api.AbstractGeoGitOp;
import org.geogit.api.Platform;
import org.geogit.api.plumbing.ResolveGeogitDir;

import com.google.common.base.Throwables;
import com.google.inject.Inject;

/**
 * Resolves the location of the {@code osm} mapping log folder directory relative to the
 * {@link Platform#pwd() current directory}.
 * <p>
 * If the folder is not found, it will be created.
 * 
 */
public class ResolveOSMMappingLogFolder extends AbstractGeoGitOp<File> {

    /**
     * Constructs a new instance of {@code ResolveOSMMappingLogFolder} with the specified platform.
     * 
     */
    @Inject
    public ResolveOSMMappingLogFolder() {

    }

    @Override
    public File call() {
        final URL geogitDirUrl = command(ResolveGeogitDir.class).call().get();
        File repoDir;
        try {
            repoDir = new File(geogitDirUrl.toURI());
        } catch (URISyntaxException e) {
            throw Throwables.propagate(e);
        }
        File osmMapFolder = new File(new File(repoDir, "osm"), "map");
        if (!osmMapFolder.exists()) {
            if (!osmMapFolder.mkdirs()) {
                throw new IllegalStateException("Could not create osm mapping log folder");
            }
        }
        return osmMapFolder;

    }
}
