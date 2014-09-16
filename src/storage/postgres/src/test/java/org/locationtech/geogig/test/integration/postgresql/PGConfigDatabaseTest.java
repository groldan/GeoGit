/* Copyright (c) 2013 OpenPlans. All rights reserved.
 * This code is licensed under the BSD New License, available at the root
 * application directory.
 */
package org.locationtech.geogig.test.integration.postgresql;

import org.locationtech.geogig.api.Platform;
import org.locationtech.geogig.storage.ConfigDatabaseTest;
import org.locationtech.geogig.storage.postgresql.PGConfigDatabase;

public class PGConfigDatabaseTest extends ConfigDatabaseTest<PGConfigDatabase>{

    @Override
    protected PGConfigDatabase createDatabase(Platform platform) {
        return new PGConfigDatabase(platform);
    }

    @Override
    protected void destroy(PGConfigDatabase config) {
        config.close();
    }

}
