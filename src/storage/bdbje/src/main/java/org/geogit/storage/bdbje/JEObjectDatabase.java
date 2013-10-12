/* Copyright (c) 2013 OpenPlans. All rights reserved.
 * This code is licensed under the BSD New License, available at the root
 * application directory.
 */
package org.geogit.storage.bdbje;

import static com.sleepycat.je.OperationStatus.NOTFOUND;
import static com.sleepycat.je.OperationStatus.SUCCESS;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import javax.annotation.Nullable;

import org.geogit.api.ObjectId;
import org.geogit.api.RevObject;
import org.geogit.storage.AbstractObjectDatabase;
import org.geogit.storage.ObjectDatabase;
import org.geogit.storage.ObjectSerializingFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;
import com.google.common.collect.UnmodifiableIterator;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.google.inject.Inject;
import com.sleepycat.collections.CurrentTransaction;
import com.sleepycat.je.Cursor;
import com.sleepycat.je.CursorConfig;
import com.sleepycat.je.Database;
import com.sleepycat.je.DatabaseConfig;
import com.sleepycat.je.DatabaseEntry;
import com.sleepycat.je.Environment;
import com.sleepycat.je.LockMode;
import com.sleepycat.je.OperationStatus;
import com.sleepycat.je.Transaction;
import com.sleepycat.je.TransactionConfig;

/**
 * 
 */
public class JEObjectDatabase extends AbstractObjectDatabase implements ObjectDatabase {

    private static final Logger LOGGER = LoggerFactory.getLogger(JEObjectDatabase.class);

    private EnvironmentBuilder envProvider;

    /**
     * Lazily loaded, do not access it directly but through {@link #getEnvironment()}
     */
    private Environment env;

    private Database objectDb;

    @Nullable
    private CurrentTransaction txn;

    @Inject
    public JEObjectDatabase(final ObjectSerializingFactory serialFactory,
            final EnvironmentBuilder envProvider) {
        super(serialFactory);
        this.envProvider = envProvider;
    }

    public JEObjectDatabase(final ObjectSerializingFactory serialFactory, final Environment env) {
        super(serialFactory);
        this.env = env;
    }

    /**
     * @return the env
     */
    private synchronized Environment getEnvironment() {
        if (env == null) {
            env = envProvider.setRelativePath("objects").get();
        }
        return env;
    }

    private ExecutorService service;

    @Override
    public synchronized void close() {
        if (env == null) {
            LOGGER.trace("Database already closed.");
            return;
        }
        final File envHome = env.getHome();
        LOGGER.debug("Closing object database at {}", envHome);
        service.shutdownNow();
        try {
            while (!service.awaitTermination(5, TimeUnit.SECONDS)) {
                LOGGER.trace("Awaiting termination of bulk insert thread pool...");
            }
        } catch (InterruptedException e) {
            LOGGER.info("Caught interrupted exception waiting for thread pool termination. "
                    + "Ignoring in order to proceed with closing the databse");
        }

        objectDb.close();
        objectDb = null;
        LOGGER.trace("ObjectDatabase closed. Closing environment...");

        env.cleanLog();
        env.sync();
        env.close();
        env = null;
        LOGGER.debug("Database {} closed.", envHome);
    }

    @Override
    public boolean isOpen() {
        return objectDb != null;
    }

    @Override
    public synchronized void open() {
        if (isOpen()) {
            LOGGER.trace("Environment {} already open", env.getHome());
            return;
        }
        // System.err.println("OPEN");
        Environment environment = getEnvironment();
        LOGGER.debug("Opening ObjectDatabase at {}", env.getHome());
        {
            // REVISIT: make thread pool size configurable?
            final int nThreads = Math.max(2, Runtime.getRuntime().availableProcessors() / 2);

            final ThreadFactory threadFactory = new ThreadFactoryBuilder().setNameFormat(
                    "BDBJE " + getEnvironment().getHome().getName() + " thread %d").build();

            int corePoolSize = nThreads;
            int maximumPoolSize = nThreads;
            long keepAliveTime = 0L;
            TimeUnit timeUnit = TimeUnit.MILLISECONDS;
            LinkedBlockingQueue<Runnable> queue = new LinkedBlockingQueue<Runnable>();
            ThreadPoolExecutor.CallerRunsPolicy rejectedExecutionHandler = new ThreadPoolExecutor.CallerRunsPolicy();
            service = new ThreadPoolExecutor(corePoolSize, maximumPoolSize, keepAliveTime,
                    timeUnit, queue, threadFactory, rejectedExecutionHandler);
            LOGGER.trace("Created bulk insert thread pool for {} with {} threads.",
                    environment.getHome(), nThreads);
        }

        txn = CurrentTransaction.getInstance(environment);

        DatabaseConfig dbConfig = new DatabaseConfig();
        dbConfig.setAllowCreate(true);
        boolean transactional = getEnvironment().getConfig().getTransactional();

        dbConfig.setDeferredWrite(!transactional);

        dbConfig.setTransactional(transactional);
        Database database = environment.openDatabase(null, "ObjectDatabase", dbConfig);
        this.objectDb = database;
        LOGGER.debug("Object database opened at {}. Transactional: {}", environment.getHome(),
                transactional);
    }

    @Override
    public <T extends RevObject> boolean put(final T object) {
        return super.put(object);
    }

    @Override
    public RevObject get(ObjectId id) throws IllegalArgumentException {
        return super.get(id);
    }

    @Override
    public <T extends RevObject> T get(ObjectId id, Class<T> type) throws IllegalArgumentException {
        return super.get(id, type);
    }

    @Override
    protected List<ObjectId> lookUpInternal(final byte[] partialId) {

        DatabaseEntry key;
        {
            byte[] keyData = partialId.clone();
            key = new DatabaseEntry(keyData);
        }

        DatabaseEntry data = new DatabaseEntry();
        data.setPartial(0, 0, true);// do not retrieve data

        List<ObjectId> matches;

        CursorConfig cursorConfig = new CursorConfig();
        cursorConfig.setReadUncommitted(true);

        Transaction transaction = txn == null ? null : txn.getTransaction();
        Cursor cursor = objectDb.openCursor(transaction, cursorConfig);
        try {
            // position cursor at the first closest key to the one looked up
            OperationStatus status = cursor.getSearchKeyRange(key, data, LockMode.READ_UNCOMMITTED);
            if (SUCCESS.equals(status)) {
                matches = new ArrayList<ObjectId>(2);
                final byte[] compKey = new byte[partialId.length];
                while (SUCCESS.equals(status)) {
                    byte[] keyData = key.getData();
                    System.arraycopy(keyData, 0, compKey, 0, compKey.length);
                    if (Arrays.equals(partialId, compKey)) {
                        matches.add(new ObjectId(keyData));
                    } else {
                        break;
                    }
                    status = cursor.getNext(key, data, LockMode.READ_UNCOMMITTED);
                }
            } else {
                matches = Collections.emptyList();
            }
            return matches;
        } finally {
            cursor.close();
        }
    }

    /**
     * @see org.geogit.storage.ObjectDatabase#exists(org.geogit.api.ObjectId)
     */
    @Override
    public boolean exists(final ObjectId id) {
        Preconditions.checkNotNull(id, "id");

        DatabaseEntry key = new DatabaseEntry(id.getRawValue());
        DatabaseEntry data = new DatabaseEntry();
        // tell db not to retrieve data
        data.setPartial(0, 0, true);

        final LockMode lockMode = LockMode.READ_UNCOMMITTED;
        Transaction transaction = txn == null ? null : txn.getTransaction();
        OperationStatus status = objectDb.get(transaction, key, data, lockMode);
        return SUCCESS == status;
    }

    @Override
    protected InputStream getRawInternal(final ObjectId id, final boolean failIfNotFound) {
        Preconditions.checkNotNull(id, "id");
        DatabaseEntry key = new DatabaseEntry(id.getRawValue());
        DatabaseEntry data = new DatabaseEntry();

        final LockMode lockMode = LockMode.READ_UNCOMMITTED;
        Transaction transaction = txn == null ? null : txn.getTransaction();
        OperationStatus operationStatus = objectDb.get(transaction, key, data, lockMode);
        if (NOTFOUND.equals(operationStatus)) {
            if (failIfNotFound) {
                throw new IllegalArgumentException("Object does not exist: " + id.toString());
            }
            return null;
        }
        final byte[] cData = data.getData();

        return new ByteArrayInputStream(cData);
    }

    private static final Comparator<RevObject> OBJECTID_COMPARATOR = new Comparator<RevObject>() {
        @Override
        public int compare(RevObject o1, RevObject o2) {
            return o1.getId().compareTo(o2.getId());
        }
    };

    @Override
    public void putAll(final Iterator<? extends RevObject> objects) {
        if (!objects.hasNext()) {
            return;
        }

        List<Future<?>> futures = Lists.newLinkedList();

        // REVISIT: make partitionSize configurable? it seems that the larger the value the longer
        // it'll take the BulkInserts to acquire the db locks and hence the larger the thread
        // contention on the db.
        final int partitionSize = 500;

        UnmodifiableIterator<?> partitions = Iterators.partition(objects, partitionSize);
        while (partitions.hasNext()) {
            @SuppressWarnings("unchecked")
            List<RevObject> partition = (List<RevObject>) partitions.next();
            partition = Lists.newArrayList(partition);

            Collections.sort(partition, OBJECTID_COMPARATOR);

            Future<?> future = putAll(partition);
            futures.add(future);
        }
        for (Future<?> future : futures) {
            try {
                future.get();
            } catch (Exception e) {
                Throwables.propagate(e);
            }
        }
    }

    private Future<?> putAll(List<RevObject> partition) {
        BulkInsert bulkInsert = new BulkInsert(partition);
        Future<?> future = service.submit(bulkInsert);
        return future;
    }

    private class BulkInsert implements Runnable {

        private List<RevObject> partition;

        public BulkInsert(List<RevObject> partition) {
            this.partition = partition;
        }

        @Override
        public void run() {
            final boolean transactional = objectDb.getConfig().getTransactional();
            Transaction transaction;
            final boolean handleTx;
            if (transactional) {
                transaction = txn.getTransaction();
                handleTx = transaction == null;
                if (handleTx) {
                    TransactionConfig txConfig = TransactionConfig.DEFAULT;
                    // txConfig = new TransactionConfig();
                    // txConfig.setDurability(Durability.COMMIT_NO_SYNC);
                    // txConfig.setReadUncommitted(true);
                    transaction = txn.beginTransaction(txConfig);
                }
            } else {
                transaction = null;
                handleTx = false;
            }

            CursorConfig cursorConfig = CursorConfig.READ_UNCOMMITTED;
            Cursor cursor = objectDb.openCursor(transaction, cursorConfig);
            try {
                ByteArrayOutputStream rawOut = new ByteArrayOutputStream();
                DatabaseEntry key = new DatabaseEntry(new byte[ObjectId.NUM_BYTES]);

                for (RevObject object : partition) {
                    rawOut.reset();
                    writeObject(object, rawOut);
                    final byte[] rawData = rawOut.toByteArray();
                    final ObjectId id = object.getId();

                    id.getRawValue(key.getData());
                    DatabaseEntry data = new DatabaseEntry(rawData);

                    cursor.putNoOverwrite(key, data);
                }
                cursor.close();
                if (transactional) {
                    if (handleTx) {
                        LOGGER.trace("Committed {} inserts to {}", partition.size(), objectDb
                                .getEnvironment().getHome());
                        txn.commitTransaction();
                    } else {
                        LOGGER.trace(
                                "Inserted {} objects, not committed, transaction not owned by this bulk inserter",
                                partition.size());
                    }
                } else {
                    // finally force an environment checkpoint to ensure durability
                    // this.env.sync();
                    // objectDb.sync();
                }
            } catch (Exception e) {
                cursor.close();
                if (transactional) {
                    txn.abortTransaction();
                }
            } finally {
                partition.clear();
                partition = null;
            }
        }

    }

    @Override
    protected boolean putInternal(final ObjectId id, final byte[] rawData) {
        OperationStatus status;
        Transaction transaction = txn == null ? null : txn.getTransaction();
        status = putInternal(id, rawData, transaction);
        final boolean didntExist = SUCCESS.equals(status);

        return didntExist;
    }

    private OperationStatus putInternal(final ObjectId id, final byte[] rawData,
            Transaction transaction) {
        OperationStatus status;
        final byte[] rawKey = id.getRawValue();
        DatabaseEntry key = new DatabaseEntry(rawKey);
        DatabaseEntry data = new DatabaseEntry(rawData);

        status = objectDb.putNoOverwrite(transaction, key, data);
        return status;
    }

    @Override
    public boolean delete(final ObjectId id) {
        final byte[] rawKey = id.getRawValue();
        final DatabaseEntry key = new DatabaseEntry(rawKey);

        Transaction transaction = txn == null ? null : txn.getTransaction();
        final OperationStatus status = objectDb.delete(transaction, key);

        return SUCCESS.equals(status);
    }

    @Override
    public long deleteAll(Iterator<ObjectId> ids) {
        long count = 0;

        CursorConfig cconfig = new CursorConfig();
        UnmodifiableIterator<List<ObjectId>> partition = Iterators.partition(ids, 10 * 1000);

        final DatabaseEntry data = new DatabaseEntry();
        data.setPartial(0, 0, true);// do not retrieve data

        while (partition.hasNext()) {
            List<ObjectId> nextIds = Lists.newArrayList(partition.next());
            Collections.sort(nextIds);

            Transaction transaction = txn == null ? null : env.beginTransaction(null,
                    TransactionConfig.DEFAULT);
            Cursor cursor = objectDb.openCursor(transaction, cconfig);
            try {
                DatabaseEntry key = new DatabaseEntry(new byte[ObjectId.NUM_BYTES]);
                for (ObjectId id : nextIds) {
                    // copy id to key object without allocating new byte[]
                    id.getRawValue(key.getData());

                    OperationStatus status = cursor.getSearchKey(key, data, LockMode.DEFAULT);
                    if (OperationStatus.SUCCESS.equals(status)) {
                        OperationStatus delete = cursor.delete();
                        if (OperationStatus.SUCCESS.equals(delete)) {
                            count++;
                        }
                    }
                }
                cursor.close();
            } catch (Exception e) {
                cursor.close();
                if (transaction != null) {
                    transaction.abort();
                }
                Throwables.propagate(e);
            }
            if (transaction != null) {
                transaction.commit();
            }
        }
        return count;
    }
}
