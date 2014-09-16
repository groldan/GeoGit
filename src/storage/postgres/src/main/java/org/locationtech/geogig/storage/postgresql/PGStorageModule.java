/* Copyright (c) 2013 OpenPlans. All rights reserved.
 * This code is licensed under the BSD New License, available at the root
 * application directory.
 */
package org.locationtech.geogig.storage.postgresql;

import org.locationtech.geogig.storage.ConfigDatabase;
import org.locationtech.geogig.storage.GraphDatabase;
import org.locationtech.geogig.storage.ObjectDatabase;
import org.locationtech.geogig.storage.StagingDatabase;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.inject.AbstractModule;
import com.google.inject.Scopes;

/**
 * Module for the Xerial SQLite storage backend.
 * <p>
 * More information about the SQLite jdbc driver available at {@link https
 * ://bitbucket.org/xerial/sqlite-jdbc}.
 * </p>
 * 
 * @author Justin Deoliveira, Boundless
 */
public class PGStorageModule extends AbstractModule {

    static Logger LOG = LoggerFactory.getLogger(PGStorageModule.class);

    @Override
    protected void configure() {
        bind(ConfigDatabase.class).to(PGConfigDatabase.class).in(Scopes.SINGLETON);
        bind(ObjectDatabase.class).to(PGObjectDatabase.class).in(Scopes.SINGLETON);
        bind(GraphDatabase.class).to(PGGraphDatabase.class).in(Scopes.SINGLETON);
        bind(StagingDatabase.class).to(PGStagingDatabase.class).in(Scopes.SINGLETON);
    }

}
