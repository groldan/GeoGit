package org.locationtech.geogig.storage.s3;

import static org.locationtech.geogig.api.ObjectId.NUM_BYTES;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;

import org.locationtech.geogig.api.ObjectId;
import org.locationtech.geogig.api.RevObject;
import org.locationtech.geogig.repository.RepositoryConnectionException;
import org.locationtech.geogig.storage.AbstractObjectDatabase;
import org.locationtech.geogig.storage.BulkOpListener;
import org.locationtech.geogig.storage.ConfigDatabase;
import org.locationtech.geogig.storage.ObjectReader;
import org.locationtech.geogig.storage.datastream.DataStreamSerializationFactoryV2;

import com.amazonaws.services.s3.model.DeleteObjectsRequest;
import com.amazonaws.services.s3.model.DeleteObjectsRequest.KeyVersion;
import com.amazonaws.services.s3.model.DeleteObjectsResult;
import com.amazonaws.services.s3.model.DeleteObjectsResult.DeletedObject;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.PutObjectResult;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.S3ObjectInputStream;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.google.common.base.Predicates;
import com.google.common.base.Throwables;
import com.google.common.collect.AbstractIterator;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;
import com.google.common.io.ByteStreams;
import com.google.inject.Provider;

public class S3ObjectDatabase extends AbstractObjectDatabase {

    private ExecutorService s3Threads;

    private ConfigDatabase configDB;

    private Provider<S3Client> clientFactory;

    private S3Client client;

    public S3ObjectDatabase(Provider<S3Client> clientFactory) {
        super(DataStreamSerializationFactoryV2.INSTANCE);
        this.clientFactory = clientFactory;
    }

    @Override
    public boolean isOpen() {
        return client != null;
    }

    @Override
    public synchronized void open() {
        if (isOpen()) {
            return;
        }
        client = clientFactory.get();
    }

    @Override
    public void close() {
        client = null;
    }

    @Override
    public void configure() throws RepositoryConnectionException {
        RepositoryConnectionException.StorageType.OBJECT.configure(configDB, "s3", "0.1");
    }

    @Override
    public void checkConfig() throws RepositoryConnectionException {
        RepositoryConnectionException.StorageType.OBJECT.verify(configDB, "s3", "0.1");
    }

    @Override
    public boolean exists(ObjectId id) {
        return submitSync(new ExistsRequest(client, id)).booleanValue();
    }

    @Override
    public boolean delete(ObjectId id) {
        List<ObjectId> deletedIds = submitSync(new DeleteRequest(client, id));
        return !deletedIds.isEmpty();
    }

    @Override
    public Iterator<RevObject> getAll(final Iterable<ObjectId> ids, final BulkOpListener listener) {
        return new AbstractIterator<RevObject>() {
            Iterator<List<ObjectId>> partitions = Iterables.partition(ids, 1000).iterator();

            Iterator<RevObject> currPartition = Iterators.emptyIterator();

            @Override
            protected RevObject computeNext() {
                if (currPartition.hasNext()) {
                    return currPartition.next();
                }
                if (partitions.hasNext()) {
                    currPartition = fetchPartition(partitions.next());
                    return computeNext();
                }
                return endOfData();
            }

            private Iterator<RevObject> fetchPartition(List<ObjectId> partition) {

                List<Future<RevObject>> futures = new ArrayList<>(partition.size());
                for (ObjectId id : partition) {
                    futures.add(submitAsync(new GetRequest(client, id, listener)));
                }
                Iterator<Future<RevObject>> iterator = futures.iterator();
                Function<Future<RevObject>, RevObject> function = new Function<Future<RevObject>, RevObject>() {
                    @Override
                    public RevObject apply(Future<RevObject> input) {
                        RevObject object;
                        try {
                            object = input.get();
                        } catch (InterruptedException | ExecutionException e) {
                            throw Throwables.propagate(Throwables.getRootCause(e));
                        }
                        return object;
                    }
                };
                return Iterators.filter(Iterators.transform(iterator, function),
                        Predicates.notNull());
            }
        };
    }

    @Override
    public long deleteAll(Iterator<ObjectId> ids, BulkOpListener listener) {
        final int partitionSize = 1000;

        Iterator<List<ObjectId>> partitions = Iterators.partition(ids, partitionSize);

        List<Future<List<ObjectId>>> futures = new ArrayList<>(100);

        while (partitions.hasNext()) {
            List<ObjectId> partition = partitions.next();

            Future<List<ObjectId>> futureResult;
            futureResult = submitAsync(new DeleteRequest(client, partition, listener));

            futures.add(futureResult);
            if (futures.size() == 100) {
                drain(futures);
            }
        }
        drain(futures);
        return 0;
    }

    // @Override
    // public void putAll(final Iterator<? extends RevObject> objects, final BulkOpListener
    // listener) {
    //
    // ByteArrayOutputStream rawOut = new ByteArrayOutputStream();
    // while (objects.hasNext()) {
    // RevObject object = objects.next();
    // rawOut.reset();
    //
    // writeObject(object, rawOut);
    // final byte[] rawData = rawOut.toByteArray();
    //
    // final ObjectId id = object.getId();
    // final boolean added = putInternal(id, rawData);
    // if (added) {
    // listener.inserted(object.getId(), rawData.length);
    // } else {
    // listener.found(object.getId(), null);
    // }
    // }
    // }

    private <T> void drain(List<Future<T>> futures) {
        List<Throwable> errors = new LinkedList<>();
        for (Iterator<Future<T>> it = futures.iterator(); it.hasNext();) {
            try {
                it.next().get();
            } catch (InterruptedException ie) {
                return;
            } catch (ExecutionException e) {
                errors.add(Throwables.getRootCause(e));
            }
        }
    }

    @Override
    protected List<ObjectId> lookUpInternal(final byte[] raw) {
        final String prefix = partialId(raw);
        List<ObjectId> matches = new LinkedList<>();
        for (S3ObjectSummary match : client.listWithPrefix(prefix)) {
            String key = match.getKey();
            matches.add(KEY_TO_ID.apply(key));
        }
        return matches;
    }

    @Override
    protected InputStream getRawInternal(ObjectId id, boolean failIfNotFound)
            throws IllegalArgumentException {
        throw new UnsupportedOperationException("Shouldn't be called. We override get() directly");
    }

    @SuppressWarnings("unchecked")
    @Override
    protected <T extends RevObject> T get(final ObjectId id, final ObjectReader<T> reader,
            boolean failIfNotFound) {
        RevObject object = submitSync(new GetRequest(client, id, failIfNotFound));
        return null == object ? null : (T) object;
    }

    @Override
    protected boolean putInternal(final ObjectId id, final byte[] rawData) {
        PutRequest req = new PutRequest(client, id, rawData);
        PutObjectResult result = submitSync(req);
        return result != null;
    }

    private <T> T submitSync(Callable<T> req) {
        Future<T> f = submitAsync(req);
        T result;
        try {
            result = f.get();
        } catch (InterruptedException | ExecutionException e) {
            throw Throwables.propagate(Throwables.getRootCause(e));
        }
        return result;
    }

    private <T> Future<T> submitAsync(Callable<T> req) {
        return s3Threads.submit(req);
    }

    private static class PutRequest implements Callable<PutObjectResult> {

        private S3Client client;

        private ObjectId id;

        private byte[] rawData;

        public PutRequest(S3Client client, ObjectId id, byte[] rawData) {
            this.client = client;
            this.id = id;
            this.rawData = rawData;
        }

        @Override
        public PutObjectResult call() throws Exception {
            final String key = ID_TO_KEY.apply(id);
            PutObjectResult result = client.putObject(key, rawData);
            return result;
        }
    }

    private static class GetRequest implements Callable<RevObject> {

        private S3Client client;

        private ObjectId id;

        private boolean failIfNotFound;

        private BulkOpListener listener;

        public GetRequest(final S3Client client, final ObjectId id, final boolean failIfNotFound) {
            this.client = client;
            this.id = id;
            this.failIfNotFound = failIfNotFound;
            this.listener = BulkOpListener.NOOP_LISTENER;
        }

        public GetRequest(final S3Client client, final ObjectId id, final BulkOpListener listener) {
            this.client = client;
            this.id = id;
            this.listener = listener;
            this.failIfNotFound = false;
        }

        @Override
        public RevObject call() throws Exception {
            final String key = ID_TO_KEY.apply(id);
            S3Object object = client.getObject(key);
            if (object == null) {
                if (failIfNotFound) {
                    throw new IllegalArgumentException("Object not found " + id);
                }
                listener.notFound(id);
                return null;
            }
            S3ObjectInputStream objectContent = object.getObjectContent();
            try {
                int contentLength = (int) object.getObjectMetadata().getContentLength();
                InternalByteArrayOutputStream to = new InternalByteArrayOutputStream(contentLength);
                ByteStreams.copy(objectContent, to);
                listener.found(id, Integer.valueOf(contentLength));
                ByteArrayInputStream in = new ByteArrayInputStream(to.bytes());
                return DataStreamSerializationFactoryV2.INSTANCE.createObjectReader().read(id, in);
            } finally {
                objectContent.close();
            }
        }
    }

    private static class ExistsRequest implements Callable<Boolean> {

        private S3Client client;

        private ObjectId id;

        public ExistsRequest(final S3Client client, final ObjectId id) {
            this.client = client;
            this.id = id;
        }

        @Override
        public Boolean call() throws Exception {
            final String key = ID_TO_KEY.apply(id);
            ObjectMetadata metadata = client.getMetadata(key);
            return Boolean.valueOf(metadata != null);
        }
    }

    /**
     * Tries to delete a list of objects and returns the list of successfully deleted ones
     */
    private static class DeleteRequest implements Callable<List<ObjectId>> {

        private S3Client client;

        private List<ObjectId> ids;

        private BulkOpListener listener;

        public DeleteRequest(final S3Client client, final ObjectId id) {
            this(client, ImmutableList.of(id), BulkOpListener.NOOP_LISTENER);
        }

        public DeleteRequest(final S3Client client, final List<ObjectId> ids,
                final BulkOpListener listener) {
            this.client = client;
            this.ids = ids;
            this.listener = listener;
        }

        @Override
        public List<ObjectId> call() throws Exception {
            List<KeyVersion> keys = Lists.transform(ids, toKeyVersion);
            List<DeletedObject> successfullyDeletedObjects = client.deleteObjects(keys);
            List<ObjectId> deleted = new ArrayList<>(Lists.transform(successfullyDeletedObjects,
                    toObjectId));
            for (ObjectId id : ids) {
                if (deleted.contains(id)) {
                    listener.deleted(id);
                } else {
                    listener.notFound(id);
                }
            }
            return deleted;
        }

        private static Function<DeletedObject, ObjectId> toObjectId = new Function<DeleteObjectsResult.DeletedObject, ObjectId>() {
            @Override
            public ObjectId apply(DeletedObject input) {
                return KEY_TO_ID.apply(input.getKey());
            }
        };

        private static Function<ObjectId, KeyVersion> toKeyVersion = new Function<ObjectId, DeleteObjectsRequest.KeyVersion>() {

            @Override
            public KeyVersion apply(ObjectId input) {
                return new KeyVersion(ID_TO_KEY.apply(input));
            }
        };
    }

    static final Function<ObjectId, String> ID_TO_KEY = new Function<ObjectId, String>() {

        @Override
        public String apply(ObjectId id) {
            StringBuilder sb = new StringBuilder(2 * NUM_BYTES);
            appendIdByte((byte) id.byteN(0), sb).append('/');
            appendIdByte((byte) id.byteN(1), sb).append('/');
            appendIdByte((byte) id.byteN(2), sb).append('/');
            appendIdByte((byte) id.byteN(3), sb).append('/');
            for (int i = 4; i < NUM_BYTES; i++) {
                appendIdByte((byte) id.byteN(i), sb);
            }
            return sb.toString();
        }
    };

    private static final String partialId(byte[] partialId) {
        Preconditions.checkArgument(partialId.length > 4, "partial id is too short");
        StringBuilder sb = new StringBuilder(2 * NUM_BYTES);
        appendIdByte(partialId[0], sb).append('/');
        appendIdByte(partialId[1], sb).append('/');
        appendIdByte(partialId[2], sb).append('/');
        appendIdByte(partialId[3], sb).append('/');
        for (int i = 4; i < partialId.length; i++) {
            appendIdByte(partialId[i], sb);
        }
        return sb.toString();
    }

    private static final char[] HEX_DIGITS = "0123456789abcdef".toCharArray();

    private static StringBuilder appendIdByte(final byte b, StringBuilder sb) {
        sb.append(HEX_DIGITS[(b >> 4) & 0xf]).append(HEX_DIGITS[b & 0xf]);
        return sb;
    }

    static final Function<String, ObjectId> KEY_TO_ID = new Function<String, ObjectId>() {
        @Override
        public ObjectId apply(final String key) {
            return ObjectId.valueOf(key.replaceAll("/", ""));
        }
    };

    private static final class InternalByteArrayOutputStream extends ByteArrayOutputStream {

        public InternalByteArrayOutputStream(int initialBuffSize) {
            super(initialBuffSize);
        }

        public byte[] bytes() {
            return super.buf;
        }

        public int size() {
            return super.count;
        }
    }
}
