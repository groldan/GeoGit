/* Copyright (c) 2013 OpenPlans. All rights reserved.
 * This code is licensed under the BSD New License, available at the root
 * application directory.
 */
package org.locationtech.geogig.storage.postgresql;

import static org.locationtech.geogig.storage.postgresql.PGStorage.VERSION3;

import org.locationtech.geogig.api.Platform;
import org.locationtech.geogig.storage.ConfigDatabase;

import com.google.inject.Inject;

/**
 * PostgreSQL graph database.
 */
public class PGGraphDatabaseV3 extends PGGraphDatabase {

    @Inject
    public PGGraphDatabaseV3(ConfigDatabase configdb, Platform platform) {
        super(configdb, platform, VERSION3);
    }
}
