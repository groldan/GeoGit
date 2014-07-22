package org.locationtech.geogig.storage.fs.mapped;

import static com.google.common.base.Preconditions.checkState;

import java.io.File;
import java.io.IOException;
import java.nio.file.DirectoryStream;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.NavigableMap;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import org.locationtech.geogig.api.ObjectId;
import org.locationtech.geogig.storage.BulkOpListener;

import com.google.common.base.Strings;
import com.google.common.base.Throwables;
import com.google.common.io.Files;
import com.google.common.util.concurrent.ThreadFactoryBuilder;

class Shard {

    private static final class InsertTask implements Callable<Boolean> {

        private byte[] k;

        private ByteData d;

        private BulkOpListener listener;

        private Shard shard;

        public InsertTask(byte[] k, ByteData d, BulkOpListener listener, Shard shard) {
            this.k = k;
            this.d = d;
            this.listener = listener;
            this.shard = shard;
        }

        @Override
        public Boolean call() throws Exception {
            Index index = shard.currentIndex();
            if (!index.add(k, d)) {
                index = shard.newIndex();
                checkState(index.add(k, d));
            }
            listener.inserted(ObjectId.createNoClone(k), d.size());
            return Boolean.TRUE;
        }
    }

    private File directory;

    private NavigableMap<Integer, Index> indexes = new ConcurrentSkipListMap<>();

    private Integer id;

    private ExecutorService writerService;

    public Shard(final File directory, Integer id) {
        checkState(directory.exists() && directory.isDirectory());
        this.directory = directory;
        this.id = id;
        writerService = Executors.newSingleThreadExecutor(new ThreadFactoryBuilder().setNameFormat(
                "shard-" + id).build());
        // System.err.println("Created shard " + directory);
    }

    public synchronized void open() {
        loadIndexes();
    }

    public synchronized void close() {
        if (writerService != null) {
            writerService.shutdown();
            try {
                writerService.awaitTermination(1, TimeUnit.MINUTES);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
        for (Index index : indexes.values()) {
            index.close();
        }
        indexes.clear();
    }

    public Future<Boolean> add(byte[] key, ByteData data, final BulkOpListener listener) {
        // if (contains(key)) {
        // return false;
        // }
        final byte[] k = key.clone();
        final ByteData d = new ByteData(data.size());
        try {
            data.writeTo(d);
        } catch (IOException e) {
            throw Throwables.propagate(e);
        }

        Future<Boolean> future = writerService.submit(new InsertTask(k, d, listener, this));
        return future;
    }

    private synchronized Index newIndex() {
        Integer indexKey;
        if (indexes.isEmpty()) {
            indexKey = Integer.valueOf(0);
        } else {
            indexKey = Integer.valueOf(1 + indexes.lastKey());
        }
        Index idx = new Index(new File(directory, Strings.padStart(indexKey.toString(), 4, '0')
                + ".idx"));
        indexes.put(indexKey, idx);
        return idx;
    }

    private Index currentIndex() {
        if (indexes.isEmpty()) {
            newIndex();
        }
        return indexes.lastEntry().getValue();
    }

    public boolean delete(byte[] key) {
        boolean deleted = false;
        for (Index index : indexes.values()) {
            deleted |= index.delete(key);
        }
        return deleted;
    }

    public boolean find(byte[] key, ByteData data) {
        for (Index index : indexes.values()) {
            if (index.find(key, data)) {
                return true;
            }
        }
        return false;
    }

    public boolean contains(byte[] key) {
        for (Index index : indexes.values()) {
            if (index.contains(key)) {
                return true;
            }
        }
        return false;
    }

    public List<byte[]> findPartial(byte[] partialId) {
        List<byte[]> objects = new ArrayList<>(2);
        for (Index index : indexes.values()) {
            List<byte[]> found = index.findPartial(partialId);
            objects.addAll(found);
        }
        return objects;
    }

    public String toString() {
        StringBuilder sb = new StringBuilder("Shard[");
        sb.append(directory.getAbsolutePath());
        sb.append(']');
        return sb.toString();
    }

    private void loadIndexes() {
        try (DirectoryStream<Path> stream = java.nio.file.Files.newDirectoryStream(
                this.directory.toPath(), "*.idx")) {
            for (Path indexFile : stream) {
                String name = Files.getNameWithoutExtension(indexFile.getFileName().toString());
                Integer key = Integer.valueOf(name);
                indexes.put(key, new Index(indexFile.toFile()));
            }
        } catch (IOException e) {
            throw Throwables.propagate(e);
        }
    }
}
