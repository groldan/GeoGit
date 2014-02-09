package org.geogit.storage.bdbje;

import java.io.File;

import org.geogit.api.ObjectId;
import org.geogit.api.Platform;
import org.geogit.api.RevFeature;
import org.geogit.api.RevObject;
import org.geogit.api.TestPlatform;
import org.geogit.storage.ConfigDatabase;
import org.geogit.storage.ObjectDatabase;
import org.geogit.storage.ObjectSerializingFactory;
import org.geogit.storage.datastream.DataStreamSerializationFactory;
import org.geogit.storage.fs.IniConfigDatabase;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.rules.TemporaryFolder;

import com.google.common.base.Optional;
import com.google.common.collect.ImmutableList;

public class JEObjectDatabaseTest extends Assert {

    protected ObjectDatabase odb;

    protected Platform platform;

    @Rule
    public TemporaryFolder tmpFolder = new TemporaryFolder();

    @Rule
    public ExpectedException exception = ExpectedException.none();

    @Before
    public void setUp() throws Exception {
        File home = tmpFolder.newFolder("home");
        File repo = tmpFolder.newFolder(".geogit");
        platform = new TestPlatform(repo);
        ((TestPlatform) platform).setUserHome(home);
        this.odb = createDatabase(platform);
        this.odb.open();
    }

    @After
    public void tearDown() throws Exception {
        if (odb != null) {
            odb.close();
        }
    }

    protected ObjectDatabase createDatabase(Platform platform) {
        ConfigDatabase configDB = new IniConfigDatabase(platform);
        ObjectSerializingFactory serialFactory = new DataStreamSerializationFactory();
        EnvironmentBuilder envProvider = new EnvironmentBuilder(platform);
        JEObjectDatabase db = new JEObjectDatabase(configDB, serialFactory, envProvider);
        return db;
    }

    @Test
    public void testPutAllIteratorOfQextendsRevObjectBulkOpListener() {
        fail("Not yet implemented");
    }

    @Test
    public void testClose() {
        assertTrue(odb.isOpen());
        odb.close();
        assertFalse(odb.isOpen());
        try {
            odb.close();
        } catch (Throwable t) {
            t.printStackTrace();
            fail("close should be idempotent");
        }
    }

    @Test
    public void testOpen() {
        odb.close();
        assertFalse(odb.isOpen());
        odb.open();
        assertTrue(odb.isOpen());
    }

    @Test
    public void testPut() {
        ImmutableList<Optional<Object>> featureProps = ImmutableList.<Optional<Object>> of(Optional
                .<Object> of("sample"));
        RevObject object = RevFeature.build(featureProps);
        assertTrue("inserting a non existing object should return true", odb.put(object));
        assertFalse("inserting an existing object should return false", odb.put(object));

        exception.expect(NullPointerException.class);
        odb.put((RevObject) null);

        RevFeature nullIdObject = new RevFeature(ObjectId.NULL, featureProps);
        exception.expect(IllegalArgumentException.class);
        exception.expectMessage("ObjectId");
        odb.put(nullIdObject);
    }

    @Test
    public void testGetAll() {
        fail("Not yet implemented");
    }

    @Test
    public void testPutAll() {
        fail("Not yet implemented");
    }

    @Test
    public void testDeleteAll() {
        fail("Not yet implemented");
    }

    @Test
    public void testExists() {
        fail("Not yet implemented");
    }

    @Test
    public void testDelete() {
        fail("Not yet implemented");
    }

    @Test
    public void testDeleteAllIteratorOfObjectIdBulkOpListener() {
        fail("Not yet implemented");
    }

    @Test
    public void testConfigure() {
        fail("Not yet implemented");
    }

    @Test
    public void testCheckConfig() {
        fail("Not yet implemented");
    }

    @Test
    public void testGetAllIterableOfObjectIdBulkOpListener() {
        fail("Not yet implemented");
    }

    @Test
    public void testLookUp() {
        fail("Not yet implemented");
    }

    @Test
    public void testGetObjectId() {
        fail("Not yet implemented");
    }

    @Test
    public void testGetIfPresentObjectId() {
        fail("Not yet implemented");
    }

    @Test
    public void testGetObjectIdClassOfT() {
        fail("Not yet implemented");
    }

    @Test
    public void testGetIfPresentObjectIdClassOfT() {
        fail("Not yet implemented");
    }

    @Test
    public void testGetTree() {
        fail("Not yet implemented");
    }

    @Test
    public void testGetFeature() {
        fail("Not yet implemented");
    }

    @Test
    public void testGetFeatureType() {
        fail("Not yet implemented");
    }

    @Test
    public void testGetCommit() {
        fail("Not yet implemented");
    }

    @Test
    public void testGetTag() {
        fail("Not yet implemented");
    }

}
