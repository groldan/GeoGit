/* Copyright (c) 2013 OpenPlans. All rights reserved.
 * This code is licensed under the BSD New License, available at the root
 * application directory.
 */
package org.locationtech.geogig.storage.postgresql;

import static com.google.common.collect.Iterables.transform;
import static java.lang.String.format;
import static org.locationtech.geogig.storage.postgresql.PGStorage.log;
import static org.locationtech.geogig.storage.postgresql.PGStorage.rollbackAndRethrow;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.sql.Array;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import javax.annotation.Nullable;
import javax.sql.DataSource;

import org.locationtech.geogig.api.ObjectId;
import org.locationtech.geogig.api.Platform;
import org.locationtech.geogig.api.RevCommit;
import org.locationtech.geogig.api.RevFeature;
import org.locationtech.geogig.api.RevFeatureType;
import org.locationtech.geogig.api.RevObject;
import org.locationtech.geogig.api.RevTag;
import org.locationtech.geogig.api.RevTree;
import org.locationtech.geogig.repository.Hints;
import org.locationtech.geogig.storage.BulkOpListener;
import org.locationtech.geogig.storage.ConfigDatabase;
import org.locationtech.geogig.storage.ObjectDatabase;
import org.locationtech.geogig.storage.ObjectInserter;
import org.locationtech.geogig.storage.ObjectSerializingFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Function;
import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import com.google.common.base.Stopwatch;
import com.google.common.base.Throwables;
import com.google.common.collect.AbstractIterator;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.ning.compress.lzf.LZFInputStream;
import com.ning.compress.lzf.LZFOutputStream;

/**
 * Base class for SQLite based object database.
 */
abstract class PGObjectDatabase implements ObjectDatabase {

    static final Logger LOG = LoggerFactory.getLogger(PGObjectDatabase.class);

    static final String OBJECTS = "objects";

    static final String STAGE = "stage";

    private static final int PUT_ALL_PARTITION_SIZE = 10_000;

    private static final int GET_ALL_PARTITION_SIZE = 50;

    final String dbName;

    final Platform platform;

    final ConfigDatabase configdb;

    final ObjectSerializingFactory serializer;

    private int getAllPartitionSize = GET_ALL_PARTITION_SIZE;

    private int putAllPartitionSize = PUT_ALL_PARTITION_SIZE;

    DataSource dataSource;

    private ExecutorService executor;

    public PGObjectDatabase(ConfigDatabase configdb, Platform platform,
            final ObjectSerializingFactory serializer) {
        this(configdb, platform, serializer, OBJECTS);
    }

    PGObjectDatabase(final ConfigDatabase configdb, final Platform platform,
            final ObjectSerializingFactory serializer, final String dbName) {
        this.configdb = configdb;
        this.platform = platform;
        this.serializer = serializer;
        this.dbName = dbName;
    }

    @Override
    public void open() {
        if (dataSource != null) {
            return;
        }
        dataSource = connect();
        init(dataSource);

        Optional<Integer> getAllFetchSize = configdb.get("postgres.getAllBatchSize", Integer.class);
        Optional<Integer> putAllBatchSize = configdb.get("postgres.putAllBatchSize", Integer.class);
        if (getAllFetchSize.isPresent()) {
            Integer fetchSize = getAllFetchSize.get();
            Preconditions.checkState(fetchSize.intValue() > 0,
                    "postgres.getAllBatchSize must be a positive integer: %s. Check your config.",
                    fetchSize);
            this.getAllPartitionSize = fetchSize;
        }
        if (putAllBatchSize.isPresent()) {
            Integer batchSize = putAllBatchSize.get();
            Preconditions.checkState(batchSize.intValue() > 0,
                    "postgres.putAllBatchSize must be a positive integer: %s. Check your config.",
                    batchSize);
            this.putAllPartitionSize = batchSize;
        }

        Optional<Integer> tpoolSize = configdb.get("postgres.threadPoolSize", Integer.class);
        if (tpoolSize.isPresent()) {
            Integer poolSize = tpoolSize.get();
            Preconditions.checkState(poolSize.intValue() > 0,
                    "postgres.threadPoolSize must be a positive integer: %s. Check your config.",
                    poolSize);
            this.threadPoolSize = poolSize;
        }

        executor = Executors.newFixedThreadPool(threadPoolSize, new ThreadFactoryBuilder()
                .setNameFormat("pg-" + dbName + "-pool-%d").setDaemon(true).build());

    }

    @Override
    public boolean isOpen() {
        return dataSource != null;
    }

    @Override
    public void close() {
        if (dataSource != null) {
            try {
                close(dataSource);
            } finally {
                dataSource = null;
            }
        }
        printStats("get()", getCount, getTimeNanos, getObjectCount);
        printStats("getAll()", getAllCount, getAllTimeNanos, getAllObjectCount);
        if (executor != null) {
            executor.shutdownNow();
        }
    }

    private void printStats(String methodName, AtomicLong callCount, AtomicLong totalTimeNanos,
            AtomicLong objectCount) {
        long callTimes = callCount.get();
        if (callTimes == 0) {
            return;
        }
        long totalMillis = TimeUnit.MILLISECONDS
                .convert(totalTimeNanos.get(), TimeUnit.NANOSECONDS);
        double avgMillis = (double) totalMillis / callTimes;
        LOG.debug(String
                .format("%s: %s call count: %,d, objects found: %,d, total time: %,dms, avg call time: %fms\n",
                        dbName, methodName, callTimes, objectCount.get(), totalMillis, avgMillis));
    }

    @Override
    public boolean exists(ObjectId id) {
        return has(id, dataSource);
    }

    @Override
    public List<ObjectId> lookUp(String partialId) {
        return Lists.newArrayList(transform(search(partialId, dataSource),
                StringToObjectId.INSTANCE));
    }

    @Override
    public RevObject get(ObjectId id) throws IllegalArgumentException {
        RevObject obj = getIfPresent(id);
        if (obj == null) {
            throw new NoSuchElementException("No object with id: " + id);
        }

        return obj;
    }

    @Override
    public <T extends RevObject> T get(ObjectId id, Class<T> type) throws IllegalArgumentException {
        return get(id, type, Hints.nil());
    }

    @Override
    public <T extends RevObject> T get(ObjectId id, Class<T> type, Hints hints)
            throws IllegalArgumentException {
        RevObject obj = getIfPresent(id, hints);
        if (obj == null) {
            throw new NoSuchElementException("No object with ids: " + id);
        }
        return type.cast(obj);
    }

    private AtomicLong getCount = new AtomicLong();

    private AtomicLong getObjectCount = new AtomicLong();

    private AtomicLong getTimeNanos = new AtomicLong();

    private AtomicLong getAllCount = new AtomicLong();

    private AtomicLong getAllObjectCount = new AtomicLong();

    private AtomicLong getAllTimeNanos = new AtomicLong();

    public int threadPoolSize = 10;

    @Override
    public RevObject getIfPresent(ObjectId id) {
        return getIfPresent(id, Hints.nil());
    }

    @Override
    public @Nullable RevObject getIfPresent(ObjectId id, Hints hints)
            throws IllegalArgumentException {
        getCount.incrementAndGet();
        Stopwatch sw = Stopwatch.createStarted();
        try {
            InputStream bytes = get(id, dataSource);
            if (bytes == null) {
                return null;
            }
            getObjectCount.incrementAndGet();
            return readObject(bytes, id, hints);
        } finally {
            sw.stop();
            getTimeNanos.addAndGet(sw.elapsed(TimeUnit.NANOSECONDS));
        }
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
    public Iterator<RevObject> getAll(Iterable<ObjectId> ids) {
        return getAll(ids, BulkOpListener.NOOP_LISTENER, Hints.nil());
    }

    private static class GetAllIterator extends AbstractIterator<RevObject> {

        private Iterator<ObjectId> ids;

        private DataSource dataSource;

        private BulkOpListener listener;

        private Hints hints;

        private PGObjectDatabase db;

        private Iterator<RevObject> delegate = Iterators.emptyIterator();

        GetAllIterator(DataSource ds, Iterator<ObjectId> ids, BulkOpListener listener, Hints hints,
                PGObjectDatabase db) {
            this.dataSource = ds;
            this.ids = ids;
            this.listener = listener;
            this.hints = hints;
            this.db = db;
        }

        @Override
        protected RevObject computeNext() {
            if (delegate.hasNext()) {
                return delegate.next();
            }
            if (ids.hasNext()) {
                delegate = nextPartition();
                return computeNext();
            }
            ids = null;
            dataSource = null;
            delegate = null;
            listener = null;
            hints = null;
            db = null;
            return endOfData();
        }

        private Iterator<RevObject> nextPartition() {
            List<Future<List<RevObject>>> list = new ArrayList<>();

            Iterator<ObjectId> ids = this.ids;
            final int getAllPartitionSize = db.getAllPartitionSize;

            for (int j = 0; j < db.threadPoolSize && ids.hasNext(); j++) {
                List<ObjectId> idList = new ArrayList<>(getAllPartitionSize);
                for (int i = 0; i < getAllPartitionSize && ids.hasNext(); i++) {
                    idList.add(ids.next());
                }
                Future<List<RevObject>> objects = db.getAll(idList, dataSource, listener, hints);
                list.add(objects);
            }
            Function<Future<List<RevObject>>, List<RevObject>> function = new Function<Future<List<RevObject>>, List<RevObject>>() {
                @Override
                public List<RevObject> apply(Future<List<RevObject>> input) {
                    try {
                        return input.get();
                    } catch (InterruptedException | ExecutionException e) {
                        e.printStackTrace();
                        throw Throwables.propagate(e);
                    }
                }
            };
            Iterable<List<RevObject>> lists = Iterables.transform(list, function);
            Iterable<RevObject> concat = Iterables.concat(lists);
            return concat.iterator();
        }
    }

    @Override
    public Iterator<RevObject> getAll(final Iterable<ObjectId> ids, final BulkOpListener listener,
            final Hints hints) {

        Iterator<ObjectId> iterator = ids.iterator();
        return new GetAllIterator(dataSource, iterator, listener, hints, this);

    }

    @Override
    public boolean put(RevObject object) {
        // CountingListener l = BulkOpListener.newCountingListener();
        // putAll(Iterators.singletonIterator(object));
        // return l.inserted() > 0;
        try {
            ObjectId id = object.getId();
            boolean inserted = put(id, writeObject(object), dataSource);
            return inserted;
        } catch (IOException | RuntimeException e) {
            throw new RuntimeException("Unable to serialize object: " + object, e);
        }
    }

    @Override
    public void putAll(Iterator<? extends RevObject> objects) {
        putAll(objects, BulkOpListener.NOOP_LISTENER);
    }

    @Override
    public boolean delete(ObjectId objectId) {
        return delete(objectId, dataSource);
    }

    @Override
    public long deleteAll(Iterator<ObjectId> ids) {
        return deleteAll(ids, BulkOpListener.NOOP_LISTENER);
    }

    @Override
    public ObjectInserter newObjectInserter() {
        return new ObjectInserter(this);
    }

    /**
     * Reads object from its binary representation as stored in the database.
     */
    protected RevObject readObject(InputStream bytes, ObjectId id, Hints hints) {
        try {
            return serializer.createObjectReader().read(id, bytes, hints);
        } catch (RuntimeException e) {
            System.err.println("Error reading object " + id);
            throw e;
        }
    }

    /**
     * Writes object to its binary representation as stored in the database.
     */
    protected byte[] writeObject(RevObject object) throws IOException {
        ByteArrayOutputStream bout = new ByteArrayOutputStream();
        LZFOutputStream cout = new LZFOutputStream(bout);
        serializer.createObjectWriter(object.getType()).write(object, cout);
        cout.close();
        byte[] bytes = bout.toByteArray();
        return bytes;
    }

    /**
     * Opens a database connection, returning the object representing connection state.
     */
    protected DataSource connect() {
        return PGStorage.newDataSource(configdb);
    }

    /**
     * Closes a database connection.
     * 
     * @param dataSource The connection object.
     */

    protected void close(DataSource ds) {
        PGStorage.closeDataSource(ds);
    }

    /**
     * Creates the object table with the following schema:
     * 
     * <pre>
     * objects(id:varchar PRIMARY KEY, object:blob)
     * </pre>
     * 
     * Implementations of this method should be prepared to be called multiple times, so must check
     * if the table already exists.
     * 
     * @param dataSource The connection object.
     */
    private void init(DataSource ds) {
        new DbOp<Void>() {
            @Override
            protected Void doRun(Connection cx) throws SQLException {
                try (ResultSet tables = cx.getMetaData().getTables(null, null, dbName, null)) {
                    if (tables.next()) {
                        return null;
                    }
                }
                String sql = format(
                        "CREATE TABLE %s (id1 INTEGER, id2 TEXT, object BYTEA, PRIMARY KEY(id1, id2))",
                        dbName);
                try (Statement statement = cx.createStatement()) {
                    statement.execute(log(sql, LOG));
                }
                return null;
            }
        }.run(ds);
    }

    /**
     * Determines if the object with the specified id exists.
     */
    private boolean has(final ObjectId id, DataSource ds) {
        return new DbOp<Boolean>() {
            @Override
            protected Boolean doRun(Connection cx) throws SQLException {
                String sql = format("SELECT count(*) FROM %s WHERE id1 = ? AND id2 = ?", dbName);
                PGId pgid = PGId.fromId(id);
                PreparedStatement ps = PGStorage.prepareStatement(cx, log(sql, LOG, id));
                ps.setInt(1, pgid.id1());
                ps.setString(2, id.toString());
                try (ResultSet rs = ps.executeQuery()) {
                    rs.next();
                    return rs.getInt(1) > 0;
                }

            }
        }.run(ds);
    }

    /**
     * Searches for objects with ids that match the speciifed partial string.
     * 
     * @param partialId The partial id.
     * 
     * @return Iterable of matches.
     */
    private Iterable<String> search(final String partialId, DataSource ds) {
        // Connection cx = newConnection(ds);
        // final ResultSet rs = new DbOp<ResultSet>() {
        // @Override
        // protected ResultSet doRun(Connection cx) throws SQLException {
        // String sql = format("SELECT id FROM %s WHERE id LIKE '%%%s%%'", dbName, partialId);
        // return cx.createStatement().executeQuery(log(sql, LOG));
        // }
        // }.run(cx);
        //
        // return new StringResultSetIterable(rs, cx);
        return ImmutableList.of();
    }

    /**
     * Retrieves the object with the specified id.
     * <p>
     * Must return <code>null</code> if no such object exists.
     * </p>
     */
    @Nullable
    private InputStream get(final ObjectId id, DataSource ds) {
        return new DbOp<InputStream>() {
            @Override
            protected InputStream doRun(Connection cx) throws SQLException {
                // String sql = format("SELECT object FROM %s WHERE id1 = ? AND id2 = ?", dbName);
                String sql = format("SELECT id2,object FROM %s WHERE id1 = ?", dbName);
                PreparedStatement ps = PGStorage.prepareStatement(cx, log(sql, LOG, id));
                ps.setFetchSize(10_000);
                PGId pgid = PGId.fromId(id);
                ps.setInt(1, pgid.id1());
                // ps.setString(2, id.toString());
                InputStream in = null;
                final String sid = id.toString();
                try (ResultSet rs = ps.executeQuery()) {
                    while (rs.next()) {
                        if (sid.equals(rs.getString(1))) {
                            byte[] bytes = rs.getBytes(2);
                            try {
                                in = new LZFInputStream(new ByteArrayInputStream(bytes));
                            } catch (IOException e) {
                                throw Throwables.propagate(e);
                            }
                            break;
                        }
                    }
                    // Preconditions.checkState(!rs.next());
                    return in;
                }

            }
        }.run(ds);
    }

    private Future<List<RevObject>> getAll(final List<ObjectId> ids, final DataSource ds,
            final BulkOpListener listener, final Hints hints) {

        GetAllOp getAllOp = new GetAllOp(ids, listener, hints, this);
        Future<List<RevObject>> future = executor.submit(getAllOp);
        return future;
    }

    private static class GetAllOp extends DbOp<List<RevObject>> implements
            Callable<List<RevObject>> {

        private List<ObjectId> queryIds;

        private BulkOpListener callback;

        private Hints hints;

        private PGObjectDatabase db;

        public GetAllOp(List<ObjectId> ids, BulkOpListener listener, Hints hints,
                PGObjectDatabase db) {
            this.queryIds = ids;
            this.callback = listener;
            this.hints = hints;
            this.db = db;
        }

        @Override
        protected List<RevObject> doRun(Connection cx) throws IOException, SQLException {
            return runInternal(cx);
        }

        private List<RevObject> runInternal(Connection cx) throws IOException, SQLException {
            final String sql = format("SELECT id2,object FROM %s WHERE id1 = ANY(?)", db.dbName);
            PreparedStatement ps = PGStorage.prepareStatement(cx, log(sql, LOG, queryIds));

            final Array array = toJDBCArray(cx, queryIds);
            final int queryCount = queryIds.size();
            ps.setFetchSize(queryCount);
            ps.setArray(1, array);

            List<RevObject> found = new ArrayList<>(queryCount);

            Stopwatch sw = Stopwatch.createStarted();
            try (ResultSet rs = ps.executeQuery()) {
                if (LOG.isTraceEnabled()) {
                    LOG.trace(String.format("Executed getAll for %,d ids in %,dms\n", queryCount,
                            sw.elapsed(TimeUnit.MILLISECONDS)));
                }
                try {
                    InputStream in;
                    ObjectId id;
                    byte[] bytes;
                    RevObject obj;
                    while (rs.next()) {
                        id = ObjectId.valueOf(rs.getString(1));
                        // only add those that are in the query set. The resultset may contain
                        // more due to id1 clashes
                        if (queryIds.remove(id)) {
                            bytes = rs.getBytes(2);
                            in = new LZFInputStream(new ByteArrayInputStream(bytes));
                            obj = db.readObject(in, id, hints);
                            found.add(obj);
                            callback.found(id, Integer.valueOf(bytes.length));
                        }
                    }
                } catch (IOException e) {
                    throw Throwables.propagate(e);
                }
            }
            sw.stop();
            if (LOG.isTraceEnabled()) {
                LOG.trace(String.format("Finished getAll for %,d out of %,d ids in %,dms\n",
                        found.size(), queryCount, sw.elapsed(TimeUnit.MILLISECONDS)));
            }
            for (ObjectId id : queryIds) {
                callback.notFound(id);
            }
            return found;
        }

        private Array toJDBCArray(Connection cx, final List<ObjectId> queryIds) throws SQLException {
            Array array;
            Object[] arr = new Object[queryIds.size()];
            Iterator<ObjectId> it = queryIds.iterator();
            for (int i = 0; it.hasNext(); i++) {
                ObjectId id = it.next();
                arr[i] = Integer.valueOf(PGId.fromId(id).id1());
            }
            array = cx.createArrayOf("integer", arr);
            return array;
        }

        @Override
        public List<RevObject> call() throws Exception {
            try {
                db.getAllCount.incrementAndGet();
                Stopwatch sw = Stopwatch.createStarted();
                List<RevObject> found = run(db.dataSource);
                db.getAllTimeNanos.addAndGet(sw.stop().elapsed(TimeUnit.NANOSECONDS));
                db.getAllObjectCount.addAndGet(found.size());
                return found;
            } finally {
                db = null;
                callback = null;
                queryIds = null;
                hints = null;
            }
        }
    }

    /**
     * Inserts or updates the object with the specified id.
     */
    private boolean put(final ObjectId id, final byte[] obj, DataSource ds) {
        return new DbOp<Boolean>() {
            @Override
            protected Boolean doRun(Connection cx) throws SQLException, IOException {
                String sql = format("INSERT INTO %s (id1, id2, object) VALUES (?,?,?)", dbName);

                PreparedStatement ps = PGStorage.prepareStatement(cx, log(sql, LOG, id, obj));
                PGId pgid = PGId.fromId(id);
                ps.setInt(1, pgid.id1());
                ps.setString(2, id.toString());
                ps.setBytes(3, obj);
                try {
                    ps.executeUpdate();
                } catch (SQLException duplicate) {
                    return Boolean.FALSE;
                }
                return Boolean.TRUE;
            }
        }.run(ds).booleanValue();
    }

    /**
     * Deletes the object with the specified id.
     * 
     * @return Flag indicating if object was actually removed.
     */

    private boolean delete(final ObjectId id, DataSource ds) {
        return new DbOp<Boolean>() {
            @Override
            protected Boolean doRun(Connection cx) throws SQLException {
                String sql = format("DELETE FROM %s WHERE id1 = ? AND id2 = ?", dbName);

                PreparedStatement ps = PGStorage.prepareStatement(cx, log(sql, LOG, id));
                PGId pgid = PGId.fromId(id);
                ps.setInt(1, pgid.id1());
                ps.setString(2, id.toString());
                int updateCount = ps.executeUpdate();
                return Boolean.valueOf(updateCount > 0);
            }
        }.run(ds).booleanValue();
    }

    /**
     * Override to optimize batch insert.
     */
    @Override
    public void putAll(final Iterator<? extends RevObject> objects, final BulkOpListener listener) {
        Preconditions.checkState(isOpen(), "No open database connection");
        new DbOp<Void>() {
            @Override
            protected boolean isAutoCommit() {
                return false;
            }

            // CREATE OR REPLACE RULE stage_ignore_duplicate_inserts AS
            // ON INSERT TO stage
            // WHERE (EXISTS ( SELECT 1
            // FROM stage
            // WHERE stage.id1 = NEW.id1 and stage.id2 = NEW.id2)) DO INSTEAD NOTHING;

            @Override
            protected Void doRun(Connection cx) throws SQLException, IOException {
                // Note we rely on <dbname>_ignore_duplicate_inserts RULE to have been created so
                // attempts to insert duplicate key pairs return zero update count instead of making
                // the whole batch of inserts fail.
                String sql = format("INSERT INTO %s (id1, id2, object) VALUES(?,?,?)", dbName);
                PreparedStatement stmt = PGStorage.prepareStatement(cx, log(sql, LOG));
                long totalObjects = 0;
                try {
                    Iterator<? extends RevObject> it = objects;
                    List<ObjectId> ids = new ArrayList<>(putAllPartitionSize);
                    // partition the objects into chunks for batch processing
                    while (it.hasNext()) {
                        RevObject obj = it.next();
                        ObjectId id = obj.getId();
                        PGId pgid = PGId.fromId(id);
                        stmt.setInt(1, pgid.id1());
                        stmt.setString(2, id.toString());
                        stmt.setBytes(3, writeObject(obj));
                        stmt.addBatch();
                        ids.add(id);
                        if (ids.size() % PUT_ALL_PARTITION_SIZE == 0) {
                            // System.err.println("Inserting " + ids.size() + " objects...");
                            // Stopwatch sw = Stopwatch.createStarted();
                            int newObjects = notifyInserted(stmt.executeBatch(), ids, listener);
                            // System.err.printf("Inserted %,d new objects in %s\n", newObjects,
                            // sw.stop());
                            totalObjects += newObjects;
                            stmt.clearParameters();
                            stmt.clearBatch();
                            ids.clear();
                            // cx.commit();
                        }
                    }
                    if (!ids.isEmpty()) {
                        totalObjects += notifyInserted(stmt.executeBatch(), ids, listener);
                    }
                    // Stopwatch sw = Stopwatch.createStarted();
                    // System.err.printf("Committing insert of %,d new objects...\n", totalObjects);
                    cx.commit();
                    // System.err.printf("%,d new objects committed in %s\n", totalObjects,
                    // sw.stop());
                } catch (SQLException e) {
                    rollbackAndRethrow(cx, e);
                }
                return null;
            }
        }.run(dataSource);
    }

    int notifyInserted(int[] inserted, List<ObjectId> objects, BulkOpListener listener) {
        int newObjects = 0;
        for (int i = 0; i < inserted.length; i++) {
            ObjectId id = objects.get(i);
            if (true || inserted[i] > 0) {
                listener.inserted(id, null);
                newObjects++;
            } else {
                listener.found(id, null);
            }
        }
        return newObjects;
    }

    /**
     * Override to optimize batch delete.
     */
    @Override
    public long deleteAll(final Iterator<ObjectId> ids, final BulkOpListener listener) {
        Preconditions.checkState(isOpen(), "No open database connection");
        return new DbOp<Long>() {
            @Override
            protected boolean isAutoCommit() {
                return false;
            }

            @Override
            protected Long doRun(Connection cx) throws SQLException, IOException {
                String sql = format("DELETE FROM %s WHERE id1 = ? AND id2 = ?", dbName);

                long count = 0;

                PreparedStatement stmt = PGStorage.prepareStatement(cx, log(sql, LOG));
                try {
                    // partition the objects into chunks for batch processing
                    Iterator<List<ObjectId>> it = Iterators.partition(ids, putAllPartitionSize);

                    while (it.hasNext()) {
                        List<ObjectId> l = it.next();
                        for (ObjectId id : l) {
                            PGId pgid = PGId.fromId(id);
                            stmt.setInt(1, pgid.id1());
                            stmt.setString(2, id.toString());
                            stmt.addBatch();
                        }

                        count += notifyDeleted(stmt.executeBatch(), l, listener);
                        stmt.clearParameters();
                        stmt.clearBatch();
                    }
                    cx.commit();
                } catch (SQLException e) {
                    rollbackAndRethrow(cx, e);
                }
                return count;
            }
        }.run(dataSource);
    }

    long notifyDeleted(int[] deleted, List<ObjectId> ids, BulkOpListener listener) {
        long count = 0;
        for (int i = 0; i < deleted.length; i++) {
            ObjectId id = ids.get(i);
            if (deleted[i] > 0) {
                count++;
                listener.deleted(id);
            } else {
                listener.notFound(id);
            }
        }
        return count;
    }

    private static class PGId {

        private int id1;

        private long id2, id3;

        public PGId(int id1, long id2, long id3) {
            this.id1 = id1;
            this.id2 = id2;
            this.id3 = id3;
        }

        public int id1() {
            return id1;
        }

        public long id2() {
            return id2;
        }

        public long id3() {
            return id3;
        }

        public static PGId fromId(ObjectId id) {
            // long id1 = (((long) id.byteN(0) << 48) //
            // + ((long) (id.byteN(1)) << 40) //
            // + ((long) (id.byteN(2)) << 32) //
            // + ((long) (id.byteN(3)) << 24) //
            // + ((long) (id.byteN(4)) << 16) //
            // + ((long) (id.byteN(5)) << 8) //
            // + ((long) (id.byteN(6)) << 0));
            //
            // long id2 = (((long) id.byteN(7) << 48) //
            // + ((long) (id.byteN(8)) << 40) //
            // + ((long) (id.byteN(9)) << 32) //
            // + ((long) (id.byteN(10)) << 24) //
            // + ((long) (id.byteN(11)) << 16) //
            // + ((long) (id.byteN(12)) << 8) //
            // + ((long) (id.byteN(13)) << 0));
            //
            // long id3 = (((long) id.byteN(14) << 40) //
            // + ((long) (id.byteN(15)) << 32) //
            // + ((long) (id.byteN(16)) << 24) //
            // + ((long) (id.byteN(17)) << 16) //
            // + ((long) (id.byteN(18)) << 8) //
            // + ((id.byteN(19)) << 0));
            //
            // return new PGId(id1, id2, id3);
            int id1 = (id.byteN(0) << 24) //
                    + (id.byteN(1) << 16) //
                    + (id.byteN(2) << 8) //
                    + (id.byteN(3) << 0);
            return new PGId(id1, 0, 0);
        }
    }

    public static void main(String[] args) {
        final int min = Integer.MIN_VALUE;
        final long max = (long) Integer.MAX_VALUE + 1;
        final int numTables = 16;
        final int step = (int) (((long) max - (long) min) / numTables);
        System.err.printf("min: %,d, max: %,d, step: %,d\n", min, max, step);
        int curr = min;
        for (int i = 0; i < numTables; i++) {
            int next = curr + step;
            System.err.printf("%,d - %,d\n", curr, next);
            curr = next;
        }

        System.err.println();
        System.err.println();

        curr = min;
        for (int i = 0; i < numTables; i++) {
            int next = curr + step;
            System.err.printf("CREATE TABLE objects_%d"
                    + " (id1 INTEGER, id2 TEXT, object BYTEA, PRIMARY KEY(id1, id2),"
                    + " CHECK (id1 >= %d AND id1 < %d) ) INHERITS(objects); \n", i, curr, next);
            curr = next;
        }

        System.err.println();
        System.err.println();

        StringBuilder f = new StringBuilder(
                "CREATE OR REPLACE FUNCTION objects_partitioning_insert_trigger()\n");
        f.append("RETURNS TRIGGER AS $$\n");
        f.append("BEGIN\n");
        curr = min;
        for (int i = 0; i < numTables; i++) {
            f.append(i == 0 ? "IF" : "ELSIF");
            int next = curr + step;
            f.append(" ( NEW.id1 >= ").append(curr);
            if (i < numTables - 1) {
                f.append(" AND NEW.id1 < ").append(next);
            }
            f.append(" ) THEN\n");
            f.append("  INSERT INTO objects_").append(i).append(" VALUES (NEW.*);\n");
            curr = next;
        }
        f.append("END IF;\n");
        f.append("RETURN NULL;\n");
        f.append("END;\n");
        f.append("$$\n");
        f.append("LANGUAGE plpgsql;\n");

        System.err.println(f);
    }
}
