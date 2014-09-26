/* Copyright (c) 2013-2014 Boundless and others.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Eclipse Distribution License v1.0
 * which accompanies this distribution, and is available at
 * https://www.eclipse.org/org/documents/edl-v10.html
 *
 * Contributors:
 * Gabriel Roldan (Boundless) - initial implementation
 */
package org.locationtech.geogig.test.performance;

import java.util.Iterator;

import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.locationtech.geogig.api.Node;
import org.locationtech.geogig.api.ObjectId;
import org.locationtech.geogig.api.Platform;
import org.locationtech.geogig.api.RevObject.TYPE;
import org.locationtech.geogig.api.RevTree;
import org.locationtech.geogig.api.RevTreeBuilderNew;
import org.locationtech.geogig.storage.ObjectDatabase;
import org.locationtech.geogig.test.integration.RepositoryTestCase;

import com.google.common.base.Stopwatch;
import com.google.common.collect.AbstractIterator;
import com.vividsolutions.jts.geom.Envelope;

public class RevTreeBuilderNewPerformanceTest extends RepositoryTestCase {

    private ObjectDatabase odb;

    public TemporaryFolder tmpFolder = new TemporaryFolder();

    private Platform platform;

    private static final ObjectId FAKE_ID = ObjectId.forString("fake");

    @Override
    protected void setUpInternal() throws Exception {
        odb = repo.objectDatabase();
        platform = repo.platform();
    }

    @Test
    public void testCreateTreeOf10K() {
        testCreateTree(10_000);
    }

    @Test
    public void testCreateTreeOf50K() {
        testCreateTree(50_000);
    }

    @Test
    public void testCreateTreeOf100K() {
        testCreateTree(100_000);
    }

    @Test
    public void testCreateTreeOf500K() {
        testCreateTree(500_000);
    }

    @Test
    public void testCreateTreeOf1M() {
        testCreateTree(1_000_000);
    }

    @Test
    public void testCreateTreeOf5M() {
        testCreateTree(5_000_000);
    }

    private void testCreateTree(final int nodeCount) {
        Iterator<Node> nodes = nodes(nodeCount);
        RevTreeBuilderNew builder = new RevTreeBuilderNew(odb, platform);

        System.err.printf("Inserting %,d nodes...", nodeCount);
        Stopwatch sw = Stopwatch.createStarted();
        while (nodes.hasNext()) {
            Node next = nodes.next();
            builder.put(next);
        }
        System.err.printf("Inserted %,d nodes in %s. Creating tree...", nodeCount, sw);
        Stopwatch sw2 = Stopwatch.createStarted();
        RevTree tree = builder.build();
        System.err.printf("Created tree with %,d nodes in %s. Total: %s\n", tree.size(),
                sw2.stop(), sw.stop());
        assertEquals(nodeCount, tree.size());
    }

    private Iterator<Node> nodes(final int size) {
        return new AbstractIterator<Node>() {
            int count = 0;

            @Override
            protected Node computeNext() {
                count++;
                if (count > size) {
                    return endOfData();
                }
                return createNode(count);
            }
        };
    }

    private static Node createNode(int i) {
        String key = "Feature." + i;
        ObjectId id = ObjectId.forString(key);
        Envelope env = new Envelope(0, 0, i, i);
        Node ref = Node.create(key, id, FAKE_ID, TYPE.FEATURE, env);
        return ref;
    }

}
