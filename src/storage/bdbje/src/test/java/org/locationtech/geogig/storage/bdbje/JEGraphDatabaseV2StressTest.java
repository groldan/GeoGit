/* Copyright (c) 2013 OpenPlans. All rights reserved.
 * This code is licensed under the BSD New License, available at the root
 * application directory.
 */
package org.locationtech.geogig.storage.bdbje;

import java.io.File;

import org.locationtech.geogig.api.TestPlatform;
import org.locationtech.geogig.repository.Hints;
import org.locationtech.geogig.storage.ConfigDatabase;
import org.locationtech.geogig.storage.GraphDatabase;
import org.locationtech.geogig.storage.GraphDatabaseStressTest;
import org.locationtech.geogig.storage.fs.IniFileConfigDatabase;

import com.google.common.base.Preconditions;

public class JEGraphDatabaseV2StressTest extends GraphDatabaseStressTest {
    // instance variable so its reused as if it were the singleton in the guice config
    private EnvironmentBuilder envProvider;

    @Override
    protected GraphDatabase createDatabase(TestPlatform platform) {
        File root = platform.pwd();
        Preconditions.checkState(new File(root, ".geogig").exists());

        envProvider = new EnvironmentBuilder(platform);

        ConfigDatabase configDB = new IniFileConfigDatabase(platform);
        return new JEGraphDatabase_v0_1(configDB, envProvider, new Hints());
    }

}
