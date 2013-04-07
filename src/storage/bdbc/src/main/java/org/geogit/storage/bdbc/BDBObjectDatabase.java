/* Copyright (c) 2011 TOPP - www.openplans.org. All rights reserved.
 * This code is licensed under the LGPL 2.1 license, available at the root
 * application directory.
 */
package org.geogit.storage.bdbc;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.util.Iterator;
import java.util.List;

import org.geogit.api.ObjectId;
import org.geogit.api.RevObject;
import org.geogit.repository.RepositoryConnectionException;
import org.geogit.storage.AbstractObjectDatabase;
import org.geogit.storage.BulkOpListener;
import org.geogit.storage.ConfigDatabase;
import org.geogit.storage.ObjectDatabase;
import org.geogit.storage.ObjectSerializingFactory;

import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import com.google.common.collect.AbstractIterator;
import com.google.common.collect.ImmutableList;
import com.google.inject.Inject;
import com.sleepycat.db.Database;
import com.sleepycat.db.DatabaseConfig;
import com.sleepycat.db.DatabaseEntry;
import com.sleepycat.db.DatabaseException;
import com.sleepycat.db.DatabaseType;
import com.sleepycat.db.Environment;
import com.sleepycat.db.Hasher;
import com.sleepycat.db.LockMode;
import com.sleepycat.db.OperationStatus;
import com.sleepycat.db.Transaction;

/**
 * @TODO: extract interface
 */
public class BDBObjectDatabase extends AbstractObjectDatabase implements ObjectDatabase {

    private BDBEnvironmentBuilder envProvider;

    private static final Transaction NO_TRANSACTION = null;

    /**
     * Lazily loaded, do not access it directly but through {@link #getEnvironment()}
     */
    private Environment env;

    private Database objectDb;

    private ConfigDatabase configDb;

    @Inject
    public BDBObjectDatabase(final ObjectSerializingFactory serialFactory,
            final BDBEnvironmentBuilder envProvider, final ConfigDatabase configDb) {
        super(serialFactory);
        this.envProvider = envProvider;
        this.configDb = configDb;
    }

    public BDBObjectDatabase(final ObjectSerializingFactory serialFactory, final Environment env) {
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

    @Override
    public void close() {
        // System.err.println("CLOSE");
        if (objectDb != null) {
            try {
                objectDb.close();
            } catch (DatabaseException e) {
                throw Throwables.propagate(e);
            }
            objectDb = null;
        }
        if (env != null) {
            // System.err.println("--> " + env.getHome());
            try {
                env.closeForceSync();
            } catch (DatabaseException e) {
                throw Throwables.propagate(e);
            }
            env = null;
        }
    }

    @Override
    public boolean isOpen() {
        return objectDb != null;
    }

    @Override
    public void open() {
        if (isOpen()) {
            return;
        }
        // System.err.println("OPEN");
        Environment environment = getEnvironment();
        // System.err.println("--> " + environment.getHome());

        DatabaseConfig dbConfig = new DatabaseConfig();
        dbConfig.setAllowCreate(true);
        boolean transactional;
        try {
            transactional = getEnvironment().getConfig().getTransactional();
        } catch (DatabaseException e) {
            throw Throwables.propagate(e);
        }
        dbConfig.setTransactional(transactional);
        Hasher hasher = new Hasher() {

            @Override
            public int hash(Database db, byte[] data, int len) {
                int hashCode = (data[0] & 0xFF)//
                        | ((data[1] & 0xFF) << 8)//
                        | ((data[2] & 0xFF) << 16)//
                        | ((data[3] & 0xFF) << 24);
                return hashCode;
            }
        };
        dbConfig.setHasher(hasher);
        dbConfig.setType(DatabaseType.HASH);

        dbConfig.setReadUncommitted(true);

        Transaction transaction = null;
        String fileName = "geogit.db";
        String databaseName = "ObjectDatabase";
        Database database;
        try {
            database = environment.openDatabase(transaction, fileName, databaseName, dbConfig);
        } catch (Exception e) {
            throw Throwables.propagate(e);
        }
        this.objectDb = database;
    }

    @Override
    protected List<ObjectId> lookUpInternal(final byte[] partialId) {
        // throw new UnsupportedOperationException("Partial id lookups are not supported");
        return ImmutableList.of();
        //
        // DatabaseEntry key;
        // {
        // byte[] keyData = partialId.clone();
        // key = new DatabaseEntry(keyData);
        // }
        //
        // DatabaseEntry data = new DatabaseEntry();
        // data.setPartial(0, 0, true);// do not retrieve data
        //
        // List<ObjectId> matches;
        //
        // CursorConfig cursorConfig = new CursorConfig();
        // cursorConfig.setReadCommitted(true);
        // cursorConfig.setReadUncommitted(false);
        //
        // Transaction transaction = txn == null ? null : txn.getTransaction();
        // Cursor cursor = objectDb.openCursor(transaction, cursorConfig);
        // try {
        // // position cursor at the first closest key to the one looked up
        // OperationStatus status = cursor.getSearchKeyRange(key, data, LockMode.DEFAULT);
        // if (SUCCESS.equals(status)) {
        // matches = new ArrayList<ObjectId>(2);
        // final byte[] compKey = new byte[partialId.length];
        // while (SUCCESS.equals(status)) {
        // byte[] keyData = key.getData();
        // System.arraycopy(keyData, 0, compKey, 0, compKey.length);
        // if (Arrays.equals(partialId, compKey)) {
        // matches.add(new ObjectId(keyData));
        // } else {
        // break;
        // }
        // status = cursor.getNext(key, data, LockMode.DEFAULT);
        // }
        // } else {
        // matches = Collections.emptyList();
        // }
        // return matches;
        // } finally {
        // cursor.close();
        // }
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
        OperationStatus status;
        try {
            status = objectDb.get(NO_TRANSACTION, key, data, lockMode);
        } catch (DatabaseException e) {
            throw Throwables.propagate(e);
        }
        return OperationStatus.SUCCESS == status;
    }

    @Override
    protected InputStream getRawInternal(final ObjectId id, final boolean failIfNotFound) {
        Preconditions.checkNotNull(id, "id");
        DatabaseEntry key = new DatabaseEntry(id.getRawValue());
        DatabaseEntry data = new DatabaseEntry();

        final LockMode lockMode = LockMode.READ_UNCOMMITTED;
        OperationStatus operationStatus;
        try {
            operationStatus = objectDb.get(NO_TRANSACTION, key, data, lockMode);
        } catch (DatabaseException e) {
            throw Throwables.propagate(e);
        }
        if (OperationStatus.NOTFOUND.equals(operationStatus)) {
            if (failIfNotFound) {
                throw new IllegalArgumentException("Object does not exist: " + id.toString());
            }
            return null;
        }
        final byte[] cData = data.getData();

        return new ByteArrayInputStream(cData);
    }

    @Override
    public void putAll(final Iterator<? extends RevObject> objects) {
        Transaction transaction = null;
        try {
            ByteArrayOutputStream rawOut = new ByteArrayOutputStream();
            while (objects.hasNext()) {
                RevObject object = objects.next();

                rawOut.reset();

                writeObject(object, rawOut);
                final byte[] rawData = rawOut.toByteArray();
                final ObjectId id = object.getId();
                putInternal(id, rawData, transaction);
            }
        } catch (Exception e) {
            throw Throwables.propagate(e);
        }
    }

    @Override
    protected boolean putInternal(final ObjectId id, final byte[] rawData) {
        OperationStatus status;
        Transaction transaction = null;
        status = putInternal(id, rawData, transaction);
        final boolean didntExist = OperationStatus.SUCCESS.equals(status);

        return didntExist;
    }

    private OperationStatus putInternal(final ObjectId id, final byte[] rawData,
            Transaction transaction) {
        OperationStatus status;
        final byte[] rawKey = id.getRawValue();
        DatabaseEntry key = new DatabaseEntry(rawKey);
        DatabaseEntry data = new DatabaseEntry(rawData);

        try {
            status = objectDb.putNoOverwrite(transaction, key, data);
        } catch (DatabaseException e) {
            throw Throwables.propagate(e);
        }
        return status;
    }

    @Override
    public boolean delete(final ObjectId id) {
        final byte[] rawKey = id.getRawValue();
        final DatabaseEntry key = new DatabaseEntry(rawKey);

        Transaction transaction = null;
        OperationStatus status;
        try {
            status = objectDb.delete(transaction, key);
        } catch (DatabaseException e) {
            throw Throwables.propagate(e);
        }

        return OperationStatus.SUCCESS.equals(status);
    }

    @Override
    public void configure() throws RepositoryConnectionException {
        RepositoryConnectionException.StorageType.OBJECT.configure(configDb, "bdb", "0.1");
    }

    @Override
    public void checkConfig() throws RepositoryConnectionException {
        RepositoryConnectionException.StorageType.OBJECT.verify(configDb, "bdb", "0.1");
    }

    @Override
    public Iterator<RevObject> getAll(final Iterable<ObjectId> ids, final BulkOpListener listener) {

        return new AbstractIterator<RevObject>() {
            private Iterator<ObjectId> it = ids.iterator();

            @Override
            protected RevObject computeNext() {
                while (it.hasNext()) {
                    ObjectId id = it.next();
                    RevObject object = getIfPresent(id);
                    if (object == null) {
                        listener.notFound(id);
                    } else {
                        return object;
                    }
                }
                return endOfData();
            }
        };
    }

    @Override
    public long deleteAll(Iterator<ObjectId> ids, BulkOpListener listener) {
        long count = 0;
        while (ids.hasNext()) {
            ObjectId id = ids.next();
            boolean deleted = delete(id);
            if (deleted) {
                listener.deleted(id);
                count++;
            } else {
                listener.notFound(id);
            }
        }
        return count;
    }
}
