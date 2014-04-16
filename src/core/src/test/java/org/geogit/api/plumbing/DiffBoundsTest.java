/* Copyright (c) 2013 OpenPlans. All rights reserved.
 * This code is licensed under the BSD New License, available at the root
 * application directory.
 */
package org.geogit.api.plumbing;

import org.geogit.api.porcelain.CommitOp;
import org.geogit.test.integration.RepositoryTestCase;
import org.junit.Test;

import com.vividsolutions.jts.geom.Envelope;

public class DiffBoundsTest extends RepositoryTestCase {

    @Override
    protected void setUpInternal() throws Exception {
        populate(true, points1, points3, points1_modified);

        points1_modified = feature(pointsType, idP1, "StringProp1_1a", new Integer(1001),
                "POINT(10 20)");
        insertAndAdd(points1_modified);
        geogit.command(CommitOp.class).call();

        points1B_modified = feature(pointsType, idP1, "StringProp1B_1a", new Integer(2000),
                "POINT(10 220)");
        insertAndAdd(points1B_modified);
        geogit.command(CommitOp.class).call();

    }

    @Test
    public void testDiffBetweenDifferentTrees() {
        Envelope diffBounds = geogit.command(DiffBounds.class).setOldVersion("HEAD~3")
                .setNewVersion("HEAD").call();

        assertEquals(diffBounds.getMinX(), 1.0, 0.0);
        assertEquals(diffBounds.getMinY(), 1.0, 0.0);
        assertEquals(diffBounds.getMaxX(), 10.0, 0.0);
        assertEquals(diffBounds.getMaxY(), 220.0, 0.0);

    }

    @Test
    public void testDiffBetweenIdenticalTrees() {
        Envelope diffBoundsEnvelope = geogit.command(DiffBounds.class).setOldVersion("HEAD")
                .setNewVersion("HEAD").call();
        assertTrue(diffBoundsEnvelope.isNull());

    }
}
