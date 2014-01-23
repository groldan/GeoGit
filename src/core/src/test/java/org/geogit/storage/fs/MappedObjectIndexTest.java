/* Copyright (c) 2013 OpenPlans. All rights reserved.
 * This code is licensed under the BSD New License, available at the root
 * application directory.
 */
package org.geogit.storage.fs;

import java.io.File;
import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.lang.management.MemoryMXBean;
import java.lang.management.MemoryUsage;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.geogit.api.Platform;
import org.geogit.api.RevFeatureBuilder;
import org.geogit.api.RevObject;
import org.geogit.api.TestPlatform;
import org.geogit.test.integration.RepositoryTestCase;
import org.geotools.data.DataUtilities;
import org.geotools.feature.simple.SimpleFeatureBuilder;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.opengis.feature.simple.SimpleFeature;
import org.opengis.feature.simple.SimpleFeatureType;

import com.google.common.base.Stopwatch;
import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;
import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.GeometryFactory;

public class MappedObjectIndexTest extends Assert {
    private static final MemoryMXBean MEMORY_MX_BEAN = ManagementFactory.getMemoryMXBean();

    @Rule
    public TemporaryFolder tempFolder = new TemporaryFolder() {
        @Override
        public File newFolder() throws IOException {
            File target = new File("target");
            File createdFolder = File.createTempFile("junit", "", target);
            createdFolder.delete();
            createdFolder.mkdir();
            return createdFolder;
        }
    };

    private ExecutorService executorService;

    private MappedObjectIndex index;

    private SimpleFeatureBuilder builder;

    @Before
    public void before() throws Exception {
        tempFolder.newFolder(".geogit");
        File workingDirectory = tempFolder.getRoot();
        Platform platform = new TestPlatform(workingDirectory);
        executorService = Executors.newFixedThreadPool(4);
        index = createIndex(platform, executorService);

        String typeSpec = RepositoryTestCase.pointsTypeSpec;
        SimpleFeatureType type = DataUtilities.createType("points", typeSpec);
        builder = new SimpleFeatureBuilder(type);
    }

    protected MappedObjectIndex createIndex(Platform platform, ExecutorService executorService) {
        return new MappedObjectIndex(platform, executorService);
    }

    @After
    public void after() {
        index.close();
        executorService.shutdownNow();
    }

    @Test
    public void testEmpty() {
        Iterator<RevObject> nodes = index.objects();
        assertNotNull(nodes);
        assertFalse(nodes.hasNext());
    }

    @Test
    public void test1k() throws Exception {
        testNodes(1000);
    }

    @Test
    public void testOrder1k() throws Exception {
        testOrder(1000);
    }

    @Test
    public void test10k() throws Exception {
        testNodes(1000 * 10);
    }

    @Test
    public void testOrder10k() throws Exception {
        testOrder(1000 * 10);
    }

    @Test
    public void test100k() throws Exception {
        int count = 1000 * 100;
        testNodes(count);
    }

    @Test
    public void testOrder100k() throws Exception {
        int count = 1000 * 100;
        testOrder(count);
    }

    @Test
    public void test1M() throws Exception {
        int count = 1000 * 1000;
        testNodes(count);
    }

    @Ignore
    @Test
    public void testOrder1M() throws Exception {
        int count = 1000 * 1000;
        testOrder(count);
    }

    @Test
    public void test5M() throws Exception {
        testNodes(1000 * 1000 * 5);
    }

    @Test
    public void test10M() throws Exception {
        testNodes(1000 * 1000 * 10);
    }

    @Test
    public void test25M() throws Exception {
        testNodes(1000 * 1000 * 25);
    }

    @Test
    public void test50M() throws Exception {
        testNodes(1000 * 1000 * 50);
    }

    private void testNodes(final int count) throws Exception {
        MemoryUsage initialMem = MEMORY_MX_BEAN.getHeapMemoryUsage();

        Stopwatch sw = new Stopwatch().start();
        for (int i = 0; i < count; i++) {
            index.add(object(i));
        }
        Iterator<RevObject> objects = index.objects();
        assertNotNull(objects);
        System.err.printf("Added %,d objects to %s index in %s. Traversing...\n", count, index
                .getClass().getSimpleName(), sw.stop());
        MemoryUsage indexCreateMem = MEMORY_MX_BEAN.getHeapMemoryUsage();

        sw.reset().start();
        int size = Iterators.size(objects);
        System.err.printf("Traversed %,d nodes in %s\n", size, sw.stop());
        MemoryUsage indexTraversedMem = MEMORY_MX_BEAN.getHeapMemoryUsage();
        final double mbFactor = 1024 * 1024;
        System.err
                .printf("Initial memory usage: %.2fMB, after creating index: %.2fMB, after traversing: %.2fMB\n",
                        (initialMem.getUsed() / mbFactor), (indexCreateMem.getUsed() / mbFactor),
                        (indexTraversedMem.getUsed() / mbFactor));
        assertEquals(count, size);
        if (count >= 1000 * 1000) {
            System.gc();
            Thread.sleep(1000);
            System.gc();
            Thread.sleep(1000);
            MemoryUsage afterGCMem = MEMORY_MX_BEAN.getHeapMemoryUsage();
            System.err.printf("Mem usage after GC: %.2fMB\n", (afterGCMem.getUsed() / mbFactor));
        }
        System.err.println();
    }

    private void testOrder(final int count) throws Exception {

        List<RevObject> expected = new ArrayList<RevObject>(count);
        for (int i = 0; i < count; i++) {
            RevObject obj = object(i);
            index.add(obj);
            expected.add(obj);
        }
        Collections.sort(expected, RevObject.NATURAL_ORDER);

        Iterator<RevObject> iterator = index.objects();
        List<RevObject> actual = Lists.newArrayList(iterator);

        assertEquals(expected.size(), actual.size());
        for (int i = 0; i < expected.size(); i++) {
            RevObject expectedObject = expected.get(i);
            RevObject actualObject = actual.get(i);
            assertEquals("At index " + i, expectedObject, actualObject);
        }
    }

    private static final GeometryFactory GF = new GeometryFactory();

    private RevObject object(int i) {
        // "sp:String,ip:Integer,pp:Point:srid=4326";
        builder.reset();
        builder.set("sp", "string " + i);
        builder.set("ip", Integer.valueOf(i));
        builder.set("pp", GF.createPoint(new Coordinate(i, i)));
        SimpleFeature feature = builder.buildFeature(String.valueOf(i));
        return RevFeatureBuilder.build(feature);
    }
}
