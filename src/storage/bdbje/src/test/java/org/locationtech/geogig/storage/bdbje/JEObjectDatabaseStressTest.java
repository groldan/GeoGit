package org.locationtech.geogig.storage.bdbje;

import org.locationtech.geogig.api.Platform;
import org.locationtech.geogig.repository.Hints;
import org.locationtech.geogig.storage.ConfigDatabase;
import org.locationtech.geogig.storage.ObjectDatabase;
import org.locationtech.geogig.storage.ObjectDatabaseStressTest;

public class JEObjectDatabaseStressTest extends ObjectDatabaseStressTest {

    @Override
    protected ObjectDatabase createDb(Platform platform, ConfigDatabase config) {

        EnvironmentBuilder envProvider = new EnvironmentBuilder(platform);
        JEObjectDatabase_v0_2 db = new JEObjectDatabase_v0_2(config, envProvider, new Hints());
        return db;
    }
}
