package org.locationtech.geogig.storage.fs.mapped;

import static com.google.common.base.Preconditions.checkArgument;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;

import org.locationtech.geogig.api.ObjectId;
import org.locationtech.geogig.storage.BulkOpListener;

class FileDb {

    private final Sharding sharding;

    private static final ThreadLocal<byte[]> IDBUFF = new ThreadLocal<byte[]>() {
        @Override
        protected byte[] initialValue() {
            return new byte[ObjectId.NUM_BYTES];
        }
    };

    public FileDb(File environment, ExecutorService executor) {
        checkArgument(environment.exists() && environment.isDirectory());
        this.sharding = new Sharding(environment);
    }

    public void close() {
        sharding.close();
        IDBUFF.remove();
    }

    public Future<Boolean> put(ObjectId id, ByteData data, BulkOpListener listener) {
        Shard shard = sharding.getShard(id.byteN(0));
        byte[] key = getKey(id);
        Future<Boolean> added = shard.add(key, data, listener);
        return added;
    }

    private static byte[] getKey(ObjectId id) {
        byte[] key = IDBUFF.get();
        id.getRawValue(key);
        return key;
    }

    public boolean delete(ObjectId id) {
        Shard shard = sharding.getShard(id.byteN(0));
        byte[] key = getKey(id);
        boolean deleted = shard.delete(key);
        return deleted;
    }

    public boolean contains(ObjectId id) {
        Shard shard = sharding.getShard(id.byteN(0));
        byte[] key = getKey(id);
        boolean found = shard.contains(key);
        return found;
    }

    public boolean find(ObjectId id, ByteData data) {
        Shard shard = sharding.getShard(id.byteN(0));
        byte[] key = getKey(id);
        boolean found = shard.find(key, data);
        return found;
    }

    public List<ObjectId> findPartial(byte[] partialId) {
        int index = partialId[0] & 0xFF;
        Shard shard = sharding.getShard(index);
        List<byte[]> rawIds = shard.findPartial(partialId);
        List<ObjectId> ids = new ArrayList<>(rawIds.size());
        for (byte[] rawid : rawIds) {
            ids.add(ObjectId.createNoClone(rawid));
        }
        return ids;
    }
}
