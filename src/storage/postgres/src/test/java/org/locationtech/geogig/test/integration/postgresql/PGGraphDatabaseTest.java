/* Copyright (c) 2013 OpenPlans. All rights reserved.
 * This code is licensed under the BSD New License, available at the root
 * application directory.
 */
package org.locationtech.geogig.test.integration.postgresql;

import org.locationtech.geogig.api.Platform;
import org.locationtech.geogig.storage.ConfigDatabase;
import org.locationtech.geogig.storage.GraphDatabase;
import org.locationtech.geogig.storage.GraphDatabaseTest;
import org.locationtech.geogig.storage.fs.IniFileConfigDatabase;
import org.locationtech.geogig.storage.postgresql.PGGraphDatabaseV2;

public class PGGraphDatabaseTest extends GraphDatabaseTest {

    @Override
    protected GraphDatabase createDatabase(Platform platform) throws Exception {
        ConfigDatabase configdb = new IniFileConfigDatabase(platform);
        return new PGGraphDatabaseV2(configdb, platform) {
            @Override
            public void close() {
                super.truncate();
                super.close();
            }
        };
    }

}
