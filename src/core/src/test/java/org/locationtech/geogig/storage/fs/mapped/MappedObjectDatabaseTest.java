package org.locationtech.geogig.storage.fs.mapped;

import static org.junit.Assert.*;
import static org.junit.Assert.fail;

import java.io.File;

import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.locationtech.geogig.api.ObjectId;
import org.locationtech.geogig.api.Platform;
import org.locationtech.geogig.api.RevCommitImpl;
import org.locationtech.geogig.api.RevObject;
import org.locationtech.geogig.api.RevPerson;
import org.locationtech.geogig.api.RevPersonImpl;
import org.locationtech.geogig.api.TestPlatform;
import org.locationtech.geogig.storage.ConfigDatabase;
import org.locationtech.geogig.storage.fs.IniFileConfigDatabase;

import com.google.common.collect.ImmutableList;

public class MappedObjectDatabaseTest {

    MappedObjectDatabase db;

    @Rule
    public TemporaryFolder tmp = new TemporaryFolder();

    @Before
    public void before() {
        File environment = tmp.newFolder(".geogit", "objects");
        Platform platform = new TestPlatform(tmp.getRoot());
        ConfigDatabase config = new IniFileConfigDatabase(platform);
        db = new MappedObjectDatabase(environment, config);
        db.open();
    }

    @After
    public void after() {
        if (db != null) {
            db.close();
        }
    }

    @Test
    public void testSimplePersistence() {
        RevObject o1 = fakeObject(1);
        RevObject o2 = fakeObject(2);
        assertTrue(db.put(o1));
        assertTrue(db.put(o2));

        assertTrue(db.exists(o1.getId()));
        assertTrue(db.exists(o2.getId()));

        db.close();
        db.open();
        assertTrue(db.exists(o1.getId()));
        assertTrue(db.exists(o2.getId()));
    }

    private RevObject fakeObject(int i) {
        ObjectId objectId = fakeId(i);
        return fakeObject(objectId);
    }

    private RevObject fakeObject(ObjectId objectId) {
        String oidString = objectId.toString();
        ObjectId treeId = ObjectId.forString("tree" + oidString);
        ImmutableList<ObjectId> parentIds = ImmutableList.of();
        RevPerson author = new RevPersonImpl("Gabriel", "groldan@boundlessgeo.com", 1000, -3);
        RevPerson committer = new RevPersonImpl("Gabriel", "groldan@boundlessgeo.com", 1000, -3);
        String message = "message " + oidString;
        return new RevCommitImpl(objectId, treeId, parentIds, author, committer, message);
    }

    private ObjectId fakeId(int i) {
        return ObjectId.forString("fakeID" + i);
    }
}
