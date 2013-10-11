package org.geogit.orientdb;

import java.io.InputStream;
import java.util.Iterator;
import java.util.List;

import org.geogit.api.ObjectId;
import org.geogit.storage.AbstractObjectDatabase;
import org.geogit.storage.ObjectSerializingFactory;

import com.google.inject.Inject;

public class OrientDBObjectDatabase extends AbstractObjectDatabase {

    @Inject
    public OrientDBObjectDatabase(ObjectSerializingFactory serializationFactory) {
        super(serializationFactory);
    }

    @Override
    public void open() {
        // TODO Auto-generated method stub

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
    public boolean delete(ObjectId objectId) {
        // TODO Auto-generated method stub
        return false;
    }

    @Override
    public long deleteAll(Iterator<ObjectId> ids) {
        // TODO Auto-generated method stub
        return 0;
    }

    @Override
    protected List<ObjectId> lookUpInternal(byte[] raw) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    protected InputStream getRawInternal(ObjectId id, boolean failIfNotFound)
            throws IllegalArgumentException {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    protected boolean putInternal(ObjectId id, byte[] rawData) {
        // TODO Auto-generated method stub
        return false;
    }

}
