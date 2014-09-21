/* Copyright (c) 2013 OpenPlans. All rights reserved.
 * This code is licensed under the BSD New License, available at the root
 * application directory.
 */
package org.locationtech.geogig.storage.postgresql;

import static org.locationtech.geogig.storage.postgresql.PGStorage.FORMAT_NAME;
import static org.locationtech.geogig.storage.postgresql.PGStorage.VERSION3;

import org.locationtech.geogig.api.Platform;
import org.locationtech.geogig.repository.RepositoryConnectionException;
import org.locationtech.geogig.storage.ConfigDatabase;
import org.locationtech.geogig.storage.ObjectDatabase;

import com.google.inject.Inject;

/**
 * Base class for SQLite based staging database.
 * 
 */
public class PGStagingDatabaseV3 extends PGStagingDatabase {

    @Inject
    public PGStagingDatabaseV3(ObjectDatabase repoDb, ConfigDatabase configdb, Platform platform) {
        super(repoDb, configdb, new PGObjectDatabaseV3(configdb, platform));
    }

    @Override
    public void configure() throws RepositoryConnectionException {
        RepositoryConnectionException.StorageType.STAGING
                .configure(configdb, FORMAT_NAME, VERSION3);
    }

    @Override
    public void checkConfig() throws RepositoryConnectionException {
        RepositoryConnectionException.StorageType.STAGING.verify(configdb, FORMAT_NAME, VERSION3);
    }

}
