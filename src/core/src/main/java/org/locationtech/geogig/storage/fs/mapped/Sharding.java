package org.locationtech.geogig.storage.fs.mapped;

import static com.google.common.base.Preconditions.*;

import java.io.File;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;

public class Sharding {

    private static final int DEFAULT_PARTITIONS = 8;

    private Map<Integer, ShardSupplier> shards = new ConcurrentHashMap<>();

    private final File environment;

    public Sharding(final File environment) {
        checkArgument(environment.exists() && environment.isDirectory());
        this.environment = environment;
        for (int p = 0; p < DEFAULT_PARTITIONS; p++) {
            final String shardDirName = Strings.padStart(Integer.toHexString(p), 2, '0');
            final File shardEnvironment = new File(environment, shardDirName);
            shards.put(Integer.valueOf(p), new ShardSupplier(shardEnvironment));
        }
    }

    public int partitions() {
        return DEFAULT_PARTITIONS;
    }

    public Shard getShard(final int firstByte) {
        Preconditions.checkArgument(firstByte > -1 && firstByte < 256);
        final int partitions = partitions();
        final Integer partition = Integer.valueOf((firstByte * partitions) / 256);
        ShardSupplier supplier = shards.get(partition);
        Shard shard = supplier.get(partition);
        return shard;
    }

    public void close() {
        for (ShardSupplier supplier : shards.values()) {
            supplier.close();
        }
    }

    private static final class ShardSupplier {

        private final File shardEnvironment;

        private Shard shard;

        public ShardSupplier(final File shardEnvironment) {
            checkArgument(shardEnvironment.isDirectory() || !shardEnvironment.exists());
            this.shardEnvironment = shardEnvironment;
        }

        public synchronized void close() {
            if (shard != null) {
                shard.close();
                shard = null;
            }
        }

        public Shard get(final Integer id) {
            if (this.shard == null) {
                this.shard = create(id);
            }
            return this.shard;
        }

        private synchronized Shard create(Integer id) {
            if (this.shard != null) {
                return this.shard;
            }
            final File env = this.shardEnvironment;
            checkState((env.exists() && env.isDirectory()) || env.mkdir(),
                    "Unable to create shard environment '%s'", env.getAbsolutePath());

            Shard shard = new Shard(env, id);
            shard.open();
            return shard;
        }

    }
}
