package org.geogit.storage.fs;

import java.io.File;
import java.util.Iterator;
import java.util.List;

import org.geogit.api.ObjectId;
import org.geogit.api.Platform;
import org.geogit.api.RevCommit;
import org.geogit.api.RevFeature;
import org.geogit.api.RevFeatureType;
import org.geogit.api.RevObject;
import org.geogit.api.RevTag;
import org.geogit.api.RevTree;
import org.geogit.api.plumbing.ResolveGeogitDir;
import org.geogit.repository.RepositoryConnectionException;
import org.geogit.storage.BulkOpListener;
import org.geogit.storage.ConfigDatabase;
import org.geogit.storage.ObjectDatabase;
import org.geogit.storage.ObjectInserter;

import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import com.google.inject.Inject;

public class MappedObjectDatabase implements ObjectDatabase {

    private Platform platform;

    private ConfigDatabase configDB;

    @Inject
    public MappedObjectDatabase(Platform platform, ConfigDatabase configDB) {
        this.platform = platform;
        this.configDB = configDB;
    }

    @Override
    public void open() {
        Optional<File> geogitDir = new ResolveGeogitDir(platform).getFile();
        Preconditions.checkState(geogitDir.isPresent(), "geogit directory couldn't be determined");
    }

    @Override
    public void configure() throws RepositoryConnectionException {
        RepositoryConnectionException.StorageType.OBJECT.configure(configDB, "mappedfile", "1.0");
    }

    @Override
    public void checkConfig() throws RepositoryConnectionException {
        RepositoryConnectionException.StorageType.OBJECT.verify(configDB, "mappedfile", "1.0");
    }

    @Override
    public boolean isOpen() {
        // TODO Auto-generated method stub
        return false;
    }

    @Override
    public void close() {
        // TODO Auto-generated method stub

    }

    @Override
    public boolean exists(ObjectId id) {
        // TODO Auto-generated method stub
        return false;
    }

    @Override
    public List<ObjectId> lookUp(String partialId) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public RevObject get(ObjectId id) throws IllegalArgumentException {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public <T extends RevObject> T get(ObjectId id, Class<T> type) throws IllegalArgumentException {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public RevObject getIfPresent(ObjectId id) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public <T extends RevObject> T getIfPresent(ObjectId id, Class<T> type)
            throws IllegalArgumentException {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public RevTree getTree(ObjectId id) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public RevFeature getFeature(ObjectId id) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public RevFeatureType getFeatureType(ObjectId id) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public RevCommit getCommit(ObjectId id) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public RevTag getTag(ObjectId id) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public boolean put(RevObject object) {
        // TODO Auto-generated method stub
        return false;
    }

    @Override
    public ObjectInserter newObjectInserter() {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public boolean delete(ObjectId objectId) {
        // TODO Auto-generated method stub
        return false;
    }

    @Override
    public Iterator<RevObject> getAllPresent(Iterable<ObjectId> ids) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public Iterator<RevObject> getAllPresent(Iterable<ObjectId> ids, BulkOpListener listener) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public void putAll(Iterator<? extends RevObject> objects) {
        // TODO Auto-generated method stub

    }

    @Override
    public void putAll(Iterator<? extends RevObject> objects, BulkOpListener listener) {
        // TODO Auto-generated method stub

    }

    @Override
    public long deleteAll(Iterator<ObjectId> ids) {
        // TODO Auto-generated method stub
        return 0;
    }

    @Override
    public long deleteAll(Iterator<ObjectId> ids, BulkOpListener listener) {
        // TODO Auto-generated method stub
        return 0;
    }

}
