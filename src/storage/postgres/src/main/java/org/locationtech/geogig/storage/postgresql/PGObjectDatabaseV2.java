/* Copyright (c) 2013 OpenPlans. All rights reserved.
 * This code is licensed under the BSD New License, available at the root
 * application directory.
 */
package org.locationtech.geogig.storage.postgresql;

import static org.locationtech.geogig.storage.postgresql.PGStorage.FORMAT_NAME;
import static org.locationtech.geogig.storage.postgresql.PGStorage.VERSION2;

import org.locationtech.geogig.api.Platform;
import org.locationtech.geogig.repository.RepositoryConnectionException;
import org.locationtech.geogig.storage.ConfigDatabase;
import org.locationtech.geogig.storage.datastream.DataStreamSerializationFactoryV2;

import com.google.inject.Inject;

/**
 * Base class for SQLite based object database.
 */
public class PGObjectDatabaseV2 extends PGObjectDatabase {

    private static final DataStreamSerializationFactoryV2 SERIALIZER = DataStreamSerializationFactoryV2.INSTANCE;

    @Inject
    public PGObjectDatabaseV2(ConfigDatabase configdb, Platform platform) {
        super(configdb, platform, SERIALIZER, OBJECTS);
    }

    PGObjectDatabaseV2(ConfigDatabase configdb, Platform platform, final String dbName) {
        super(configdb, platform, SERIALIZER, dbName);
    }

    @Override
    public void configure() throws RepositoryConnectionException {
        RepositoryConnectionException.StorageType.OBJECT.configure(configdb, FORMAT_NAME, VERSION2);
    }

    @Override
    public void checkConfig() throws RepositoryConnectionException {
        RepositoryConnectionException.StorageType.OBJECT.verify(configdb, FORMAT_NAME, VERSION2);
    }

}
