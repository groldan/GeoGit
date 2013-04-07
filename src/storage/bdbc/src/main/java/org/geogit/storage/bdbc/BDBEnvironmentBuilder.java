/* Copyright (c) 2011 TOPP - www.openplans.org. All rights reserved.
 * This code is licensed under the LGPL 2.1 license, available at the root
 * application directory.
 */
package org.geogit.storage.bdbc;

import java.io.File;
import java.net.URISyntaxException;
import java.net.URL;

import org.geogit.api.Platform;
import org.geogit.api.plumbing.ResolveGeogitDir;

import com.google.common.base.Throwables;
import com.google.inject.Inject;
import com.google.inject.Provider;
import com.sleepycat.db.Environment;
import com.sleepycat.db.EnvironmentConfig;

public class BDBEnvironmentBuilder implements Provider<Environment> {

    @Inject
    private Platform platform;

    private String[] path;

    private File absolutePath;

    public BDBEnvironmentBuilder setRelativePath(String... path) {
        this.path = path;
        this.absolutePath = null;
        return this;
    }

    public BDBEnvironmentBuilder setAbsolutePath(File absolutePath) {
        this.absolutePath = absolutePath;
        this.path = null;
        return this;
    }

    /**
     * @return
     * @see com.google.inject.Provider#get()
     */
    @Override
    public synchronized Environment get() {

        final URL repoUrl = new ResolveGeogitDir(platform).call();
        if (repoUrl == null && absolutePath == null) {
            throw new IllegalStateException("Can't find geogit repository home");
        }
        final File storeDirectory;

        if (absolutePath != null) {
            storeDirectory = absolutePath;
        } else {
            File currDir;
            try {
                currDir = new File(repoUrl.toURI());
            } catch (URISyntaxException e) {
                throw Throwables.propagate(e);
            }
            File dir = currDir;
            for (String subdir : path) {
                dir = new File(dir, subdir);
            }
            storeDirectory = dir;
        }

        if (!storeDirectory.exists() && !storeDirectory.mkdirs()) {
            throw new IllegalStateException("Unable to create Environment directory: '"
                    + storeDirectory.getAbsolutePath() + "'");
        }
        EnvironmentConfig envCfg = new EnvironmentConfig();
        envCfg.setAllowCreate(true);
        
        boolean transactional = false;
        envCfg.setTransactional(transactional);
        envCfg.setInitializeLocking(transactional);

        envCfg.setInitializeCache(true);

        Environment env;
        try {
            env = new Environment(storeDirectory, envCfg);
        } catch (Exception e) {
            throw Throwables.propagate(e);
        }
        return env;
    }

}
