package org.locationtech.geogig.storage.performance.mongo;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.locationtech.geogig.api.Platform;
import org.locationtech.geogig.storage.ConfigDatabase;
import org.locationtech.geogig.storage.ObjectDatabase;
import org.locationtech.geogig.storage.ObjectDatabaseStressTest;
import org.locationtech.geogig.storage.integration.mongo.TestConfigDatabase;
import org.locationtech.geogig.storage.mongo.MongoConnectionManager;
import org.locationtech.geogig.storage.mongo.MongoObjectDatabase;

public class MongoObjectDatabaseStressTest extends ObjectDatabaseStressTest {

    @Override
    protected ObjectDatabase createDb(Platform platform, ConfigDatabase config) {

        final MongoConnectionManager manager = new MongoConnectionManager();
        final ExecutorService executor = Executors.newFixedThreadPool(4);

        MongoObjectDatabase db = new MongoObjectDatabase(new TestConfigDatabase(platform), manager,
                executor) {
            @Override
            public void close() {
                super.close();
                executor.shutdownNow();
            }
        };
        return db;
    }

}
