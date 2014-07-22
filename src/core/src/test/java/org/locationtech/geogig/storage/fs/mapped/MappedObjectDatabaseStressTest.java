package org.locationtech.geogig.storage.fs.mapped;

import static org.junit.Assert.assertTrue;

import java.io.File;

import org.locationtech.geogig.api.Platform;
import org.locationtech.geogig.storage.ConfigDatabase;
import org.locationtech.geogig.storage.ObjectDatabase;
import org.locationtech.geogig.storage.ObjectDatabaseStressTest;

public class MappedObjectDatabaseStressTest extends ObjectDatabaseStressTest {

    @Override
    protected ObjectDatabase createDb(Platform platform, ConfigDatabase config) {

        File environment = new File(new File(platform.pwd(), ".geogig"), "objects");
        assertTrue(environment.mkdirs());
        MappedObjectDatabase db = new MappedObjectDatabase(environment, config);
        return db;
    }
}
