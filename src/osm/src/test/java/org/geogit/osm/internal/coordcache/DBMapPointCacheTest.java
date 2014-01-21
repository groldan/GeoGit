/* Copyright (c) 2013 OpenPlans. All rights reserved.
 * This code is licensed under the BSD New License, available at the root
 * application directory.
 */
package org.geogit.osm.internal.coordcache;

import org.geogit.api.Platform;
import org.geogit.osm.internal.coordcache.DBMapPointCache;
import org.junit.Ignore;

@Ignore
public class DBMapPointCacheTest extends PointCacheTest {

    @Override
    protected DBMapPointCache createCache(Platform platform) {
        return new DBMapPointCache(platform);
    }

}
