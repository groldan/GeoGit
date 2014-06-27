/* Copyright (c) 2013 OpenPlans. All rights reserved.
 * This code is licensed under the BSD New License, available at the root
 * application directory.
 */
package org.geogit.repository;

import static com.google.common.base.Preconditions.checkState;
import static com.google.common.base.Predicates.notNull;
import static com.google.common.collect.Iterators.filter;
import static com.google.common.collect.Iterators.mergeSorted;
import static com.google.common.collect.Iterators.transform;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInput;
import java.io.DataInputStream;
import java.io.DataOutput;
import java.io.DataOutputStream;
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
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.geogit.api.Node;
import org.geogit.api.Platform;
import org.geogit.api.plumbing.ResolveGeogitDir;
import org.geogit.storage.NodePathStorageOrder;
import org.geogit.storage.datastream.FormatCommonV2;

import com.google.common.base.Function;
import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import com.google.common.collect.AbstractIterator;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Ordering;
import com.google.common.io.ByteStreams;
import com.google.common.io.Closeables;
import com.google.common.primitives.UnsignedLong;
import com.vividsolutions.jts.geom.Envelope;

class MappedNodeIndex implements NodeIndex {

    private static final Random RANDOM = new Random();

    private static final long MAX_BUFF_SIZE = 1 * 1024 * 1024 * 1024; // Integer.MAX_VALUE;

    private ExecutorService executor;

    private File parentDir;

    private MappedData data;

    private MappedIndex index;

    private static class MappedData {

        private final NodeSerializer SERIALIZER = new NodeSerializer();

        private File dataFile;

        private RandomAccessFile randomAccessFile;

        private FileChannel dataChannel;

        private MappedByteBuffer dataBuffer;

        private List<MappedByteBuffer> dataBuffers;

        private ReadWriteLock lock = new ReentrantReadWriteLock();

        public MappedData(File parentDir) throws IOException {
            dataFile = new File(parentDir, "nodes.0");
            dataFile.deleteOnExit();
            checkState(dataFile.createNewFile());
            randomAccessFile = new RandomAccessFile(dataFile, "rw");
            dataChannel = randomAccessFile.getChannel();
            dataBuffers = new ArrayList<MappedByteBuffer>(2);
            newBuffer();
        }

        private void newBuffer() throws IOException {
            long position = MAX_BUFF_SIZE * dataBuffers.size();
            long size = MAX_BUFF_SIZE;
            MappedByteBuffer buffer = dataChannel.map(MapMode.READ_WRITE, position, size);
            dataBuffers.add(buffer);
            this.dataBuffer = buffer;
        }

        public void close() {
            try {
                Closeables.close(dataChannel, true);
                Closeables.close(randomAccessFile, true);
            } catch (IOException e) {
                //
            }
            dataBuffers.clear();
            dataBuffer = null;
            dataChannel = null;
            dataFile.delete();
        }

        private boolean writing = true;

        public long put(Node node) {
            Preconditions.checkState(writing);
            int buffOffset;
            lock.writeLock().lock();
            final int buffIdx;
            try {
                buffOffset = SERIALIZER.write(node, dataBuffer);
            } catch (BufferOverflowException needAnewBuffer) {
                try {
                    dataBuffer.limit(dataBuffer.position());
                    newBuffer();
                    buffOffset = SERIALIZER.write(node, dataBuffer);
                } catch (IOException e) {
                    close();
                    throw Throwables.propagate(e);
                }
            } finally {
                buffIdx = dataBuffers.size() - 1;
                lock.writeLock().unlock();
            }
            long offsetBase = buffIdx * MAX_BUFF_SIZE;
            long offset = offsetBase + buffOffset;
            return offset;
        }

        public Node nodeAtOffset(final long offset) {
            lock.readLock().lock();
            final ByteBuffer buffer;
            final int buffOffset;
            try {
                if (writing) {
                    this.dataBuffer.flip();
                    writing = false;
                }
                final int bufferIndex = (int) (offset / MAX_BUFF_SIZE);
                buffOffset = (int) (offset % MAX_BUFF_SIZE);
                buffer = dataBuffers.get(bufferIndex);
            } finally {
                lock.readLock().unlock();
            }

            // use a view of the buffer to favor concurrency
            ByteBuffer buff = buffer.duplicate();
            buff.position(buffOffset);
            Node node = SERIALIZER.read(buff);
            return node;
        }

        private static class NodeSerializer {

            private InternalByteArrayOutputStream outStream = new InternalByteArrayOutputStream(512);

            private Envelope envbuff = new Envelope();

            public synchronized int write(Node node, ByteBuffer buff) throws BufferOverflowException {
                outStream.reset();
                DataOutput out = new DataOutputStream(outStream);
                try {
                    FormatCommonV2.writeNode(node, out, envbuff);
                } catch (IOException e) {
                    throw Throwables.propagate(e);
                }
                final byte[] data = outStream.bytes();
                final int size = outStream.size();

                if (buff.remaining() < size) {
                    throw new BufferOverflowException();
                }

                int offset = buff.position();
                buff.putShort((short) size);
                buff.put(data, 0, size);
                return offset;
            }

            public Node read(ByteBuffer buff) {
                final int size = buff.getShort();
                byte[] data = new byte[size];
                buff.get(data);
                DataInput in = ByteStreams.newDataInput(data);
                Node node;
                try {
                    node = FormatCommonV2.readNode(in);
                } catch (IOException e) {
                    throw Throwables.propagate(e);
                }
                return node;
            }
        }
    }

    private static class MappedIndex {

        private static final NodePathStorageOrder nodeOrder = new NodePathStorageOrder();

        private static final int PARTITION_SIZE = 10 * 1000;

        private File indexFile;

        private RandomAccessFile randomAccessFile;

        private FileChannel indexChannel;

        private MappedByteBuffer index;

        private List<MappedByteBuffer> indexBuffers;

        public MappedIndex(File parentDir) throws IOException {
            indexFile = new File(parentDir, "nodes.idx");
            indexFile.deleteOnExit();
            checkState(indexFile.createNewFile());

            randomAccessFile = new RandomAccessFile(indexFile, "rw");
            indexChannel = randomAccessFile.getChannel();
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
            try {
                Closeables.close(indexChannel, true);
                Closeables.close(randomAccessFile, true);
            } catch (IOException e) {
                //
            }
            index = null;
            indexChannel = null;
            indexFile.delete();
        }

        public void put(final String name, final long dataOffset) {
            UnsignedLong nodeHashCode = nodeOrder.hashCodeLong(name);
            put(new Entry(nodeHashCode, dataOffset));
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

        public Iterator<Long> offsets() {
            List<Iterator<Entry>> sortedEntriedByBuffer = new ArrayList<Iterator<Entry>>();

            for (int bufferIndex = 0; bufferIndex < indexBuffers.size(); bufferIndex++) {
                ByteBuffer buffer = indexBuffers.get(bufferIndex);
                List<Iterator<Entry>> bufferOffsets = offsets(buffer);
                sortedEntriedByBuffer.addAll(bufferOffsets);
            }

            final Function<Entry, Long> offsets = new Function<Entry, Long>() {
                @Override
                public Long apply(Entry e) {
                    return Long.valueOf(e.getOffset());
                }
            };
            Iterator<Entry> entriesSortedByHashcode;
            entriesSortedByHashcode = mergeSorted(sortedEntriedByBuffer, Ordering.natural());
            // ImmutableList<Entry> entries = ImmutableList.copyOf(entriesSortedByHashcode);
            // System.err.println(entries);
            // entriesSortedByHashcode = entries.iterator();
            return transform(entriesSortedByHashcode, offsets);
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

            public static final int RECSIZE = 16;// sizeOf(long) + sizeOf(long)

            public final UnsignedLong hashCode;

            public final long offset;

            public Entry(UnsignedLong nodeHashCode, long offset) {
                this.hashCode = nodeHashCode;
                this.offset = offset;
            }

            public UnsignedLong getHashCode() {
                return hashCode;
            }

            public long getOffset() {
                return offset;
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
                buffer.putLong(entry.getHashCode().longValue());
                buffer.putLong(entry.getOffset());
            }

            public static Entry read(ByteBuffer buffer) {
                long hashCode = buffer.getLong();
                long offset = buffer.getLong();
                UnsignedLong ulong = UnsignedLong.fromLongBits(hashCode);
                return new Entry(ulong, offset);
            }

        }
    }

    public MappedNodeIndex(Platform platform, ExecutorService executor) {
        this.executor = executor;

        final Optional<File> geogitDir = new ResolveGeogitDir(platform).getFile();
        checkState(geogitDir.isPresent());
        this.parentDir = new File(new File(geogitDir.get(), "tmp"), "nodeindex_"
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

    @Override
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

    @Override
    public void add(Node node) {
        long dataOffset = data.put(node);
        index.put(node.getName(), dataOffset);
    }

    @Override
    public synchronized Iterator<Node> nodes() {

        index.sort(executor);

        Iterator<Long> offsets = index.offsets();

        Function<Long, Node> offsetNode = new Function<Long, Node>() {
            @Override
            public Node apply(Long offset) {
                return data.nodeAtOffset(offset.longValue());
            }
        };
        Iterator<Node> nodes = filter(transform(offsets, offsetNode), notNull());
        return nodes;
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
    }

    private static class ByteBufferDataInputAdapter extends InputStream {

        private ByteBuffer byteBuff;

        ByteBufferDataInputAdapter(ByteBuffer buff) {
            this.byteBuff = buff;
        }

        @Override
        public int read() throws IOException {
            return byteBuff.hasRemaining() ? (byteBuff.get() & 0xFF) : -1;
        }

        @Override
        public int read(byte b[], int off, int len) throws IOException {
            int readCount = -1;
            if (byteBuff.hasRemaining()) {
                readCount = Math.min(len, byteBuff.remaining());
                byteBuff.get(b, off, readCount);
            }
            return readCount;
        }
    }
}
