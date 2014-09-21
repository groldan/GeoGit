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
 * Module for the PostgreSQL storage backend.
 */
public class PGStorageModule extends AbstractModule {

    static Logger LOG = LoggerFactory.getLogger(PGStorageModule.class);

    @Override
    protected void configure() {
        bind(ConfigDatabase.class).to(PGConfigDatabase.class).in(Scopes.SINGLETON);
        bind(ObjectDatabase.class).to(PGObjectDatabaseV2.class).in(Scopes.SINGLETON);
        bind(GraphDatabase.class).to(PGGraphDatabaseV2.class).in(Scopes.SINGLETON);
        bind(StagingDatabase.class).to(PGStagingDatabaseV2.class).in(Scopes.SINGLETON);
    }

}
