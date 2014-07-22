package org.locationtech.geogig.storage.fs.mapped;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

import java.io.File;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

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
import org.locationtech.geogig.storage.datastream.DataStreamSerializationFactoryV2;

import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import com.google.common.collect.AbstractIterator;

public class MappedObjectDatabase implements ObjectDatabase {

    private static final ThreadLocal<ByteData> DATABUFF = new ThreadLocal<ByteData>() {
        @Override
        protected ByteData initialValue() {
            return new ByteData(2048);
        }
    };

    private ConfigDatabase configDB;

    private FileDb db;

    private ExecutorService executor;

    private final Marshaller marshaller = new Marshaller(DataStreamSerializationFactoryV2.INSTANCE);

    private File environment;

    public MappedObjectDatabase(final File environment, final ConfigDatabase config) {
        checkNotNull(environment);
        checkNotNull(config);
        checkArgument(environment.exists() && environment.isDirectory(),
                "Database environment directory does not exist: %s", environment.getAbsolutePath());

        this.environment = environment;
        this.configDB = config;
    }

    public MappedObjectDatabase(Platform platform, ConfigDatabase config, String name) {
        Optional<File> file = new ResolveGeogigDir(platform).getFile();
        checkArgument(file.isPresent());
        this.environment = new File(file.get(), name);
        checkArgument((environment.exists() && environment.isDirectory()) || environment.mkdir());
        this.configDB = config;
    }

    @Override
    public void open() {
        executor = Executors.newFixedThreadPool(4);
        db = new FileDb(environment, executor);
    }

    @Override
    public boolean isOpen() {
        return db != null;
    }

    @Override
    public synchronized void close() {
        if (!isOpen()) {
            return;
        }
        executor.shutdownNow();
        executor = null;
        db.close();
        db = null;
        DATABUFF.remove();
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
        boolean found = db.contains(id);
        return found;
    }

    @Override
    public List<ObjectId> lookUp(String partialId) {
        Preconditions.checkNotNull(partialId);

        byte[] raw = ObjectId.toRaw(partialId);

        List<ObjectId> baseResults = db.findPartial(raw);

        // If the length of the partial string is odd, then the last character wasn't considered in
        // the lookup, we need to filter the list further.
        if (partialId.length() % 2 != 0) {
            Iterator<ObjectId> listIterator = baseResults.iterator();
            while (listIterator.hasNext()) {
                ObjectId result = listIterator.next();
                if (!result.toString().startsWith(partialId)) {
                    listIterator.remove();
                }
            }
        }
        return baseResults;
    }

    @Override
    public RevObject getIfPresent(ObjectId id) {
        ByteData databuff = DATABUFF.get();
        boolean found = db.find(id, databuff);
        RevObject obj;
        if (found) {
            obj = marshaller.unmarshal(id, databuff);
        } else {
            obj = null;
        }
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
        try {
            return putInternal(object, BulkOpListener.NOOP_LISTENER).get().booleanValue();
        } catch (InterruptedException | ExecutionException e) {
            throw Throwables.propagate(Throwables.getRootCause(e));
        }
    }

    @Override
    public boolean delete(ObjectId objectId) {
        boolean deleted = db.delete(objectId);
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
        List<Future<Boolean>> futures = new ArrayList<>(10_000);
        while (objects.hasNext()) {
            futures.add(putInternal(objects.next(), listener));
            if (futures.size() == 10_000) {
                purge(futures);
            }
        }
        purge(futures);
    }

    private void purge(List<Future<Boolean>> futures) {
        Exception error = null;
        for (Future<Boolean> f : futures) {
            try {
                f.get();
            } catch (InterruptedException | ExecutionException e) {
                error = e;
            }
        }
        futures.clear();
        if (error != null) {
            throw Throwables.propagate(error);
        }
    }

    private Future<Boolean> putInternal(RevObject object, BulkOpListener listener) {
        ObjectId id = object.getId();
        ByteData data = DATABUFF.get();
        marshaller.marshal(object, data);
        Future<Boolean> added = db.put(id, data, listener);
        if("c069a102c198984080c36f4318b0f8f013a85c64".equals(id.toString())){
            System.err.println("--- ADDED c069a102c198984080c36f4318b0f8f013a85c64 ---");
        }
        return added;
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
