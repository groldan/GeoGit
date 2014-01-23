/* Copyright (c) 2013 OpenPlans. All rights reserved.
 * This code is licensed under the BSD New License, available at the root
 * application directory.
 */
package org.geogit.storage.fs;

import static com.google.common.base.Preconditions.checkState;
import static com.google.common.base.Predicates.notNull;
import static com.google.common.collect.Iterators.filter;
import static com.google.common.collect.Iterators.mergeSorted;
import static com.google.common.collect.Iterators.transform;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.RandomAccessFile;
import java.nio.BufferOverflowException;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileChannel.MapMode;
import java.util.AbstractList;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Random;
import java.util.RandomAccess;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;

import org.geogit.api.ObjectId;
import org.geogit.api.Platform;
import org.geogit.api.RevObject;
import org.geogit.api.plumbing.ResolveGeogitDir;
import org.geogit.storage.NodePathStorageOrder;
import org.geogit.storage.ObjectReader;
import org.geogit.storage.ObjectWriter;
import org.geogit.storage.datastream.DataStreamSerializationFactory;

import com.google.common.base.Function;
import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import com.google.common.base.Stopwatch;
import com.google.common.base.Throwables;
import com.google.common.collect.AbstractIterator;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Ordering;
import com.google.common.io.Closeables;

public class MappedObjectIndex {

    private static final Random RANDOM = new Random();

    private static final long MAX_BUFF_SIZE = 1 * 1024 * 1024 * 1024; // Integer.MAX_VALUE;

    private ExecutorService executor;

    private File parentDir;

    private MappedData data;

    private MappedIndex index;

    private static class MappedData {

        private static final ObjectSerializer SERIALIZER = new ObjectSerializer();

        private File dataFile;

        private FileChannel dataChannel;

        private MappedByteBuffer dataBuffer;

        private List<MappedByteBuffer> dataBuffers;

        public MappedData(File parentDir) throws IOException {
            dataFile = new File(parentDir, "objects");
            dataFile.deleteOnExit();
            checkState(dataFile.createNewFile());
            dataChannel = new RandomAccessFile(dataFile, "rw").getChannel();
            dataBuffers = new ArrayList<MappedByteBuffer>(2);
            newBuffer();
        }

        private void newBuffer() throws IOException {
            long position = MAX_BUFF_SIZE * dataBuffers.size();
            long size = MAX_BUFF_SIZE;
            MappedByteBuffer buffer = dataChannel.map(MapMode.READ_WRITE, position, size);
            dataBuffers.add(buffer);
            this.dataBuffer = buffer;
            // System.err.println("Allocated new data buffer of size " + MAX_BUFF_SIZE
            // + " starting at position " + position);
        }

        public void close() {
            Closeables.closeQuietly(dataChannel);
            dataBuffers.clear();
            dataBuffer = null;
            dataChannel = null;
            dataFile.delete();
        }

        private boolean writing = true;

        public synchronized MappedIndex.Entry put(RevObject object) {
            Preconditions.checkState(writing);
            MappedIndex.Entry entry;
            try {
                entry = SERIALIZER.write(object, dataBuffer);
            } catch (BufferOverflowException needAnewBuffer) {
                try {
                    dataBuffer.limit(dataBuffer.position());
                    newBuffer();
                    entry = SERIALIZER.write(object, dataBuffer);
                } catch (IOException e) {
                    close();
                    throw Throwables.propagate(e);
                }
            }
            final int buffIdx = dataBuffers.size() - 1;
            long offsetBase = buffIdx * MAX_BUFF_SIZE;
            int buffOffset = (int) entry.getOffset();
            long offset = offsetBase + buffOffset;
            entry.setOffset(offset);
            {
                ByteBuffer check = dataBuffer.duplicate();
                check.position(buffOffset);
                int size = entry.getSize();
                check.limit(buffOffset + size);
                ByteBufferInputStream in = new ByteBufferInputStream(check);
                RevObject read;
                try {
                    read = SERIALIZER.read(object.getId(), check);
                } catch (Exception e) {
                    e.printStackTrace();
                    throw Throwables.propagate(e);
                }
                Preconditions.checkState(object.equals(read));
            }
            return entry;
        }

        public RevObject objectAtOffset(final ObjectId id, final long offset) {
            if (writing) {
                dataBuffer.flip();
                writing = false;
            }
            final int bufferIndex = (int) (offset / MAX_BUFF_SIZE);
            final int buffOffset = (int) (offset % MAX_BUFF_SIZE);
            ByteBuffer dataBuffer = dataBuffers.get(bufferIndex);

            // use a view of the buffer to favor concurrency
            ByteBuffer buff = dataBuffer.duplicate();
            buff.position(buffOffset);
            ByteBufferInputStream in = new ByteBufferInputStream(buff);
            RevObject object = SERIALIZER.read(id, buff);
            return object;
        }

        private static class ObjectSerializer {

            InternalByteArrayOutputStream out = new InternalByteArrayOutputStream(4096);

            DataStreamSerializationFactory STREAMFAC = new DataStreamSerializationFactory();

            public MappedIndex.Entry write(RevObject object, ByteBuffer buff)
                    throws BufferOverflowException {
                ObjectWriter<RevObject> writer = STREAMFAC.createObjectWriter(object.getType());
                try {
                    out.reset();
                    writer.write(object, out);
                } catch (IOException e) {
                    throw Throwables.propagate(e);
                }

                final int offset = buff.position();

                byte[] rawValue = out.bytes();
                int size = out.size();
                if (buff.remaining() < size) {
                    throw new BufferOverflowException();
                }
                buff.put(rawValue, 0, size);
                return new MappedIndex.Entry(object.getId(), offset, size);
            }

            public RevObject read(ObjectId id, ByteBuffer buff) {
                ObjectReader<RevObject> reader = STREAMFAC.createObjectReader();
                ByteBufferInputStream in = new ByteBufferInputStream(buff);
                RevObject object = reader.read(id, in);
                return object;
            }
        }
    }

    private static class MappedIndex {

        private static final NodePathStorageOrder nodeOrder = new NodePathStorageOrder();

        private static final int PARTITION_SIZE = 10 * 1000;

        private File parentDir;

        private File indexFile;

        private FileChannel indexChannel;

        private MappedByteBuffer index;

        private List<MappedByteBuffer> indexBuffers;

        public MappedIndex(File parentDir) throws IOException {
            this.parentDir = parentDir;
            indexFile = new File(parentDir, "nodes.idx");
            indexFile.deleteOnExit();
            checkState(indexFile.createNewFile());

            indexChannel = new RandomAccessFile(indexFile, "rw").getChannel();
            this.indexBuffers = new ArrayList<MappedByteBuffer>(2);
            newBuffer();
        }

        private void newBuffer() throws IOException {
            long position = MAX_BUFF_SIZE * indexBuffers.size();
            long size = MAX_BUFF_SIZE;
            MappedByteBuffer buff = indexChannel.map(MapMode.READ_WRITE, position, size);
            indexBuffers.add(buff);
            this.index = buff;
            // System.err.println("Allocated new index buffer of size " + MAX_BUFF_SIZE
            // + " starting at position " + position);
        }

        public void close() {
            Closeables.closeQuietly(indexChannel);
            index = null;
            indexChannel = null;
            indexFile.delete();
        }

        private synchronized void put(Entry entry) {
            try {
                Entry.write(index, entry);
            } catch (BufferOverflowException neeedsNewBuffer) {
                try {
                    index.limit(index.position());
                    newBuffer();
                    Entry.write(index, entry);
                } catch (IOException e) {
                    throw Throwables.propagate(e);
                }
            }
        }

        public void sort(ExecutorService executor) {
            try {
                for (ByteBuffer buffer : this.indexBuffers) {
                    buffer.flip();

                    final int limit = buffer.limit();
                    final int size = limit / MappedIndex.Entry.RECSIZE;
                    if (size < 2) {
                        return;
                    }

                    int partitionSize;
                    // partitionSize = PARTITION_SIZE / 2;
                    // if (partitionSize > limit) {
                    // sort(partitionSize, executor);
                    // }
                    partitionSize = PARTITION_SIZE;
                    sort(buffer, partitionSize, executor);
                }
            } catch (Exception e) {
                e.printStackTrace();
                throw Throwables.propagate(e);
            }
        }

        private void sort(ByteBuffer index, final int partitionSize, ExecutorService executor)
                throws InterruptedException {

            final int limit = index.limit();
            final int size = limit / MappedIndex.Entry.RECSIZE;

            final int numPartitions = 1 + (size / partitionSize);
            List<Callable<Void>> tasks = new ArrayList<Callable<Void>>(numPartitions);

            for (int i = 0; i < numPartitions; i++) {
                int fromIndex = i * partitionSize * Entry.RECSIZE;
                int toIndex = Math.min(limit, fromIndex + partitionSize * Entry.RECSIZE);

                index.position(fromIndex);
                index.limit(toIndex);
                if (index.remaining() > 0) {
                    ByteBuffer viewBuff = index.slice();
                    List<Entry> sublist = new EntryList(viewBuff);
                    tasks.add(new SortTask(sublist));
                }
            }

            executor.invokeAll(tasks);

            index.rewind();
        }

        public Iterator<Entry> offsets() {
            List<Iterator<Entry>> sortedEntriedByBuffer = new ArrayList<Iterator<Entry>>();

            for (int bufferIndex = 0; bufferIndex < indexBuffers.size(); bufferIndex++) {
                ByteBuffer buffer = indexBuffers.get(bufferIndex);
                List<Iterator<Entry>> bufferOffsets = offsets(buffer);
                sortedEntriedByBuffer.addAll(bufferOffsets);
            }

            Iterator<Entry> entriesSortedByHashcode;
            entriesSortedByHashcode = mergeSorted(sortedEntriedByBuffer, Ordering.natural());

            return entriesSortedByHashcode;
        }

        private List<Iterator<Entry>> offsets(ByteBuffer buffer) {
            final int numRecords = buffer.limit() / Entry.RECSIZE;
            if (numRecords == 0) {
                return ImmutableList.<Iterator<Entry>> of();
            }

            final int numPartitions = 1 + numRecords / PARTITION_SIZE;

            List<Iterator<Entry>> iterators = new ArrayList<Iterator<Entry>>(numPartitions);
            for (int p = 0; p < numPartitions; p++) {
                ByteBuffer view = buffer.duplicate();
                view.position(Entry.RECSIZE * p * PARTITION_SIZE);
                view.limit(Math.min(view.limit(), view.position() + Entry.RECSIZE * PARTITION_SIZE));
                if (view.remaining() > 0) {
                    iterators.add(new EntryIterator(view));
                }
            }
            return iterators;
        }

        private static class SortTask implements Callable<Void> {
            private List<Entry> list;

            public SortTask(List<MappedIndex.Entry> list) {
                this.list = list;
            }

            @Override
            public Void call() throws Exception {
                Collections.sort(list);
                return null;
            }

        }

        private static class EntryIterator extends AbstractIterator<Entry> {

            private ByteBuffer view;

            public EntryIterator(ByteBuffer view) {
                this.view = view;
            }

            @Override
            protected Entry computeNext() {
                int remaining = view.remaining();
                if (remaining == 0) {
                    return endOfData();
                }
                Entry e = Entry.read(view);
                return e;
            }

        }

        private static final class EntryList extends AbstractList<Entry> implements RandomAccess {

            private ByteBuffer buffer;

            public EntryList(ByteBuffer buffer) {
                this.buffer = buffer;
            }

            @Override
            public Entry get(int index) {
                int offset = index * Entry.RECSIZE;
                buffer.position(offset);
                Entry entry = Entry.read(buffer);
                return entry;
            }

            @Override
            public Entry set(int index, Entry element) {
                ByteBuffer buffer = this.buffer;
                final int offset = index * Entry.RECSIZE;
                buffer.position(offset);
                // MappedIndex.Entry prev = serializer.read(buffer);
                // buffer.position(offset);
                Entry.write(buffer, element);
                // return prev;
                return null;
            }

            @Override
            public int size() {
                int limit = buffer.limit();
                int size = limit / MappedIndex.Entry.RECSIZE;
                return size;
            }

        }

        private static class Entry implements Comparable<Entry> {

            public static final int RECSIZE = 32;// sizeOf(ObjectId) + sizeOf(long) + sizeOf(int)

            public final ObjectId hashCode;

            public long offset;

            private final int size;

            public Entry(ObjectId hashCode, long offset, int size) {
                this.hashCode = hashCode;
                this.offset = offset;
                this.size = size;
            }

            public void setOffset(long offset) {
                this.offset = offset;
            }

            public ObjectId getHashCode() {
                return hashCode;
            }

            public long getOffset() {
                return offset;
            }

            public int getSize() {
                return size;
            }

            @Override
            public int compareTo(Entry o) {
                return getHashCode().compareTo(o.getHashCode());
            }

            @Override
            public boolean equals(Object o) {
                if (o == this)
                    return true;
                if (!(o instanceof Entry)) {
                    return false;
                }
                Entry e = (Entry) o;
                return getHashCode() == e.getHashCode() && getOffset() == e.getOffset();
            }

            @Override
            public String toString() {
                return new StringBuilder("Entry[hash: ").append(getHashCode()).append(", offset: ")
                        .append(getOffset()).append(']').toString();
            }

            public static void write(ByteBuffer buffer, Entry entry) {
                if (buffer.remaining() < Entry.RECSIZE) {
                    throw new BufferOverflowException();
                }
                buffer.put(entry.getHashCode().getRawValue());
                buffer.putLong(entry.getOffset());
                buffer.putInt(entry.getSize());
            }

            public static Entry read(ByteBuffer buffer) {
                byte[] raw = new byte[ObjectId.NUM_BYTES];
                buffer.get(raw);
                ObjectId hashCode = ObjectId.createNoClone(raw);
                long offset = buffer.getLong();
                int size = buffer.getInt();
                return new Entry(hashCode, offset, size);
            }

        }
    }

    public MappedObjectIndex(Platform platform, ExecutorService executor) {
        this.executor = executor;

        final Optional<File> geogitDir = new ResolveGeogitDir(platform).getFile();
        checkState(geogitDir.isPresent());
        this.parentDir = new File(new File(geogitDir.get(), "tmp"), "revobjectindex_"
                + Math.abs(RANDOM.nextInt()));
        checkState(parentDir.exists() || parentDir.mkdirs());
        this.parentDir.deleteOnExit();

        try {
            this.data = new MappedData(parentDir);
            this.index = new MappedIndex(parentDir);
        } catch (IOException e) {
            throw Throwables.propagate(e);
        }
    }

    public synchronized void close() {
        if (index == null) {
            return;
        }
        try {
            index.close();
        } finally {
            try {
                data.close();
            } finally {
                index = null;
                data = null;
            }
        }
        parentDir.delete();
    }

    public int addAll(Iterator<RevObject> objects) {
        int count = 0;
        while (objects.hasNext()) {
            add(objects.next());
            count++;
        }
        return count;
    }

    public void add(RevObject object) {
        MappedIndex.Entry entry = data.put(object);
        index.put(entry);
    }

    public synchronized Iterator<RevObject> objects() {
        Stopwatch sw = new Stopwatch().start();
        // System.err.printf("sorting index...");
        index.sort(executor);
        // System.err.printf("index sorted in %s\n", sw.stop());
        Iterator<MappedIndex.Entry> offsets = index.offsets();

        // ImmutableList<Long> l = ImmutableList.copyOf(offsets);
        // System.err.println(l);
        // offsets = l.iterator();

        Function<MappedIndex.Entry, RevObject> offsetNode = new Function<MappedIndex.Entry, RevObject>() {
            @Override
            public RevObject apply(MappedIndex.Entry e) {
                return data.objectAtOffset(e.getHashCode(), e.getOffset());
            }
        };
        Iterator<RevObject> objects = filter(transform(offsets, offsetNode), notNull());
        return objects;
    }

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

        public InternalByteArrayOutputStream clear() {
            super.reset();
            return this;
        }
    }

    private static class ByteBufferInputStream extends InputStream {

        private ByteBuffer buffer;

        public ByteBufferInputStream(ByteBuffer buffer) {
            this.buffer = buffer;

        }

        @Override
        public int read() throws IOException {
            int remaining = buffer.remaining();
            if (remaining == 0) {
                return -1;
            }
            return buffer.get() & 0xFF;
        }

        @Override
        public int read(byte b[], int off, int len) throws IOException {
            int remaining = buffer.remaining();
            if (remaining == 0) {
                return -1;
            }
            int size = Math.min(len, remaining);
            buffer.get(b, off, size);
            return size;
        }

        @Override
        public int available() throws IOException {
            return buffer.remaining();
        }
    }
}
