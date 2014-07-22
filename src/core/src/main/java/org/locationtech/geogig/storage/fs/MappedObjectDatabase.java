package org.locationtech.geogig.storage.fs;

import java.io.File;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.locationtech.geogig.api.ObjectId;
import org.locationtech.geogig.api.Platform;
import org.locationtech.geogig.api.RevCommit;
import org.locationtech.geogig.api.RevFeature;
import org.locationtech.geogig.api.RevFeatureType;
import org.locationtech.geogig.api.RevObject;
import org.locationtech.geogig.api.RevTag;
import org.locationtech.geogig.api.RevTree;
import org.locationtech.geogig.api.plumbing.ResolveGeogigDir;
import org.locationtech.geogig.repository.RepositoryConnectionException;
import org.locationtech.geogig.storage.BulkOpListener;
import org.locationtech.geogig.storage.ConfigDatabase;
import org.locationtech.geogig.storage.ObjectDatabase;
import org.locationtech.geogig.storage.ObjectInserter;

import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import com.google.common.collect.AbstractIterator;
import com.google.inject.Inject;

public class MappedObjectDatabase implements ObjectDatabase {

    private Platform platform;

    private ConfigDatabase configDB;

    private MappedObjectIndex index;

    private ExecutorService executor;

    @Inject
    public MappedObjectDatabase(Platform platform, ConfigDatabase configDB) {
        this.platform = platform;
        this.configDB = configDB;
    }

    @Override
    public void open() {
        Optional<File> geogigDir = new ResolveGeogigDir(platform).getFile();
        Preconditions.checkState(geogigDir.isPresent(), "geogig directory couldn't be determined");

        executor = Executors.newFixedThreadPool(4);
        index = new MappedObjectIndex(platform, executor);

    }

    @Override
    public boolean isOpen() {
        return index != null;
    }

    @Override
    public synchronized void close() {
        if (!isOpen()) {
            return;
        }
        executor.shutdownNow();
        executor = null;
        index.close();
        index = null;
    }

    @Override
    public void configure() throws RepositoryConnectionException {
        RepositoryConnectionException.StorageType.OBJECT.configure(configDB, "file", "2.0");
    }

    @Override
    public void checkConfig() throws RepositoryConnectionException {
        RepositoryConnectionException.StorageType.OBJECT.verify(configDB, "file", "2.0");
    }

    @Override
    public boolean exists(ObjectId id) {
        return index.exists(id);
    }

    @Override
    public List<ObjectId> lookUp(String partialId) {
        return null;
    }

    @Override
    public RevObject getIfPresent(ObjectId id) {
        RevObject obj = index.find(id);
        return obj;
    }

    @Override
    public RevObject get(ObjectId id) throws IllegalArgumentException {
        RevObject object = getIfPresent(id);
        Preconditions.checkArgument(object != null, "Object does not exist %s", id);
        return object;
    }

    @Override
    public <T extends RevObject> T get(ObjectId id, Class<T> type) throws IllegalArgumentException {
        RevObject obj = get(id);
        Preconditions.checkArgument(type.isAssignableFrom(obj.getClass()));
        return type.cast(obj);
    }

    @Override
    public <T extends RevObject> T getIfPresent(ObjectId id, Class<T> type)
            throws IllegalArgumentException {
        RevObject obj = getIfPresent(id);
        if (obj == null) {
            return null;
        }
        Preconditions.checkArgument(type.isAssignableFrom(obj.getClass()));
        return type.cast(obj);
    }

    @Override
    public RevTree getTree(ObjectId id) {
        return get(id, RevTree.class);
    }

    @Override
    public RevFeature getFeature(ObjectId id) {
        return get(id, RevFeature.class);
    }

    @Override
    public RevFeatureType getFeatureType(ObjectId id) {
        return get(id, RevFeatureType.class);
    }

    @Override
    public RevCommit getCommit(ObjectId id) {
        return get(id, RevCommit.class);
    }

    @Override
    public RevTag getTag(ObjectId id) {
        return get(id, RevTag.class);
    }

    @Override
    public ObjectInserter newObjectInserter() {
        return new ObjectInserter(this);
    }

    @Override
    public boolean put(RevObject object) {
        // @todo return proper value
        index.add(object);
        return true;
    }

    @Override
    public boolean delete(ObjectId objectId) {
        boolean deleted = index.delete(objectId);
        return deleted;
    }

    @Override
    public Iterator<RevObject> getAll(Iterable<ObjectId> ids) {
        return getAll(ids, BulkOpListener.NOOP_LISTENER);
    }

    @Override
    public Iterator<RevObject> getAll(final Iterable<ObjectId> ids, final BulkOpListener listener) {
        return new AbstractIterator<RevObject>() {

            private Iterator<ObjectId> idIt = ids.iterator();

            @Override
            protected RevObject computeNext() {
                while (idIt.hasNext()) {
                    ObjectId id = idIt.next();
                    RevObject object = getIfPresent(id);
                    if (object == null) {
                        listener.notFound(id);
                    } else {
                        listener.found(id, null);
                        return object;
                    }
                }
                return endOfData();
            }
        };
    }

    @Override
    public void putAll(Iterator<? extends RevObject> objects) {
        putAll(objects, BulkOpListener.NOOP_LISTENER);
    }

    @Override
    public void putAll(Iterator<? extends RevObject> objects, BulkOpListener listener) {
        while (objects.hasNext()) {
            RevObject object = objects.next();
            if (put(object)) {
                listener.inserted(object.getId(), null);
            } else {
                listener.found(object.getId(), null);
            }
        }
    }

    @Override
    public long deleteAll(Iterator<ObjectId> ids) {
        return deleteAll(ids, BulkOpListener.NOOP_LISTENER);
    }

    @Override
    public long deleteAll(Iterator<ObjectId> ids, BulkOpListener listener) {
        long cnt = 0;
        while (ids.hasNext()) {
            ObjectId id = ids.next();
            if (delete(id)) {
                listener.deleted(id);
                cnt++;
            } else {
                listener.notFound(id);
            }
        }
        return cnt;
    }

}
