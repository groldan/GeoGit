/* Copyright (c) 2013 OpenPlans. All rights reserved.
 * This code is licensed under the BSD New License, available at the root
 * application directory.
 */
package org.locationtech.geogig.storage.postgresql;

import static org.locationtech.geogig.storage.postgresql.PGStorage.VERSION2;

import org.locationtech.geogig.api.Platform;
import org.locationtech.geogig.storage.ConfigDatabase;

import com.google.inject.Inject;

/**
 * PostgreSQL graph database.
 */
public class PGGraphDatabaseV2 extends PGGraphDatabase {

    @Inject
    public PGGraphDatabaseV2(ConfigDatabase configdb, Platform platform) {
        super(configdb, platform, VERSION2);
    }
}
