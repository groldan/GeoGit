/* Copyright (c) 2013 OpenPlans. All rights reserved.
 * This code is licensed under the GNU GPL 2.0 license, available at the root
 * application directory.
 */
package org.locationtech.geogig.test.integration;

import org.junit.Test;
import org.locationtech.geogig.api.NodeRef;
import org.locationtech.geogig.api.hooks.GeoGigAPI;
import org.locationtech.geogig.api.porcelain.CommitOp;
import org.opengis.feature.Feature;

public class GeogigAPITest extends RepositoryTestCase {

    private GeoGigAPI geogigAPI;

    @Override
    protected void setUpInternal() throws Exception {
        geogigAPI = new GeoGigAPI(this.repo);
    }

    @Test
    public void testGetFeaturesToCommit() throws Exception {
        insertAndAdd(points1, points2);
        Feature[] features = geogigAPI.getFeaturesToCommit(null, false);
        assertEquals(2, features.length);
    }

    @Test
    public void testGetFeatureFromHead() throws Exception {
        insertAndAdd(points1);
        geogig.command(CommitOp.class).setMessage("Commit message").call();
        Feature feature = geogigAPI.getFeatureFromHead(NodeRef.appendChild(pointsName, idP1));
        assertNotNull(feature);
    }

    @Test
    public void testGetFeatureFromWorkingTree() throws Exception {
        insert(points1);
        Feature feature = geogigAPI
                .getFeatureFromWorkingTree(NodeRef.appendChild(pointsName, idP1));
        assertNotNull(feature);
    }

}