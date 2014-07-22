package org.locationtech.geogig.storage.fs.mapped;

import java.io.IOException;
import java.nio.BufferOverflowException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.IntBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileChannel.MapMode;
import java.util.AbstractList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.RandomAccess;

import javax.annotation.Nullable;

import org.locationtech.geogig.api.ObjectId;

import sun.nio.ch.DirectBuffer;

import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.google.common.collect.Iterators;
import com.google.common.collect.Ordering;
import com.google.common.collect.Range;
import com.google.common.hash.BloomFilter;
import com.google.common.hash.Funnels;
import com.google.common.primitives.UnsignedBytes;

class Page {

    static final Ordering<byte[]> ID_ORDER = Ordering.from(UnsignedBytes
            .lexicographicalComparator());

    public static final int HEADER_SIZE = 4;

    public static final int ENTRY_SIZE = 1 + 4 + 4 + ObjectId.NUM_BYTES;

    private static final ThreadLocal<byte[]> BUFF = new ThreadLocal<byte[]>();

    /** the actual page buffer */
    private ByteBuffer buffer;

    /** A view used to write and synchronize upon on write operations */
    private ByteBuffer writeView;

    /** A view on the buffer's header indicating how many object ids are in the page */
    private IntBuffer numObjects;

    int pageIndex;

    byte[] min, max;

    private volatile boolean packed = false;

    private BloomFilter<byte[]> bloomFilter;

    public Page(ByteBuffer buffer, int pageIndex) {
        this.buffer = buffer;
        this.pageIndex = pageIndex;

        IntBuffer intBuffer = buffer.asIntBuffer();
        intBuffer.limit(1);
        this.numObjects = intBuffer.slice();

        buffer.position(HEADER_SIZE);
        buffer.limit(buffer.capacity());
        this.writeView = buffer.slice();
        final int numObjects = numObjects();
        this.bloomFilter = newBloomFilter();
        if (numObjects > 0) {
            List<byte[]> ids = asIdList();
            for (Iterator<byte[]> it = ids.iterator(); it.hasNext();) {
                byte[] id = it.next();
                bloomFilter.put(id);
                if (min == null) {
                    min = id;
                    max = id;
                } else {
                    min = ID_ORDER.min(min, id);
                    max = ID_ORDER.max(max, id);
                }
            }
        }
        this.writeView.position(ENTRY_SIZE * numObjects);
    }

    private BloomFilter<byte[]> newBloomFilter() {
        return BloomFilter.create(Funnels.byteArrayFunnel(), 1024 * 10, 0.001);
    }

    public int numObjects() {
        int numObjects = this.numObjects.get(0);
        return numObjects;
    }

    public boolean delete(byte[] key) {
        synchronized (writeView) {
            int numObjects = numObjects();
            FileChannel c;
            return false;
        }
    }

    public int pageIndex() {
        return pageIndex;
    }

    public int indexOf(byte[] key) {
        if (!packed) {
            pack();
        }
        if (min == null) {
            return -1;
        }
        int comp = ID_ORDER.compare(min, key);
        if (comp > 0) {
            return -1;
        }
        comp = ID_ORDER.compare(max, key);
        if (comp < 0) {
            return -1;
        }

        if (!bloomFilter.mightContain(key)) {
            return -1;
        }

        List<byte[]> list = asIdList();
        int index = Collections.binarySearch(list, key, ID_ORDER);
        if (index < 0) {
            return -1;
        }
        return index;
    }

    @Nullable
    public Entry find(final byte[] key) {
        final int index = indexOf(key);
        if (index < 0) {
            return null;
        }
        List<Entry> entries = asEntryList();
        Entry entry = entries.get(index);
        return entry;
    }

    public Result add(byte[] key, final int offset, final int size) {
        synchronized (writeView) {
//            final int index = indexOf(key);// Collections.binarySearch(list, key, ID_ORDER);
//            if (index >= 0) {
//                return Result.KEY_EXISTS;
//            }
            final int maxSize = writeView.capacity() / ENTRY_SIZE;
            List<Entry> entries = asEntryList();
            if (entries.size() == maxSize) {
                return Result.PAGE_FULL;
            }

            // final int insertionPoint = Math.abs(index) - 1;
            Entry entry = new Entry(key, offset, size, (byte) 1);
            // insertion sort
            // entries.add(insertionPoint, entry);
            entries.add(entry);
            packed = false;
            this.numObjects.put(0, entries.size());
            bloomFilter.put(key);
            byte[] keyClone = key.clone();
            if (min == null) {
                min = keyClone;
                max = keyClone;
            } else {
                min = ID_ORDER.min(keyClone, min);
                max = ID_ORDER.max(keyClone, max);
            }
        }
        return Result.SUCCESS;
    }

    public static Page create(FileChannel indexChannel, final int pageIndex, final int pageSize)
            throws IOException {
        // Preconditions.checkArgument((pageSize - HEADER_SIZE) % ENTRY_SIZE == 0);
        Preconditions.checkArgument(pageSize % 4096 == 0);
        long pagePosition = (long) pageSize * pageIndex;
        MappedByteBuffer buffer = indexChannel.map(MapMode.READ_WRITE, pagePosition, pageSize);
        buffer.order(ByteOrder.BIG_ENDIAN);
        System.err.println("Created page " + pageIndex + "/" + pageSize + " at position "
                + pagePosition);
        return new Page(buffer, pageIndex);
    }

    public static Page create(ByteBuffer indexBuffer, final int pageIndex, final int pageSize)
            throws IOException {
        // Preconditions.checkArgument((pageSize - HEADER_SIZE) % ENTRY_SIZE == 0);
        // Preconditions.checkArgument(pageSize % 4096 == 0);
        int pagePosition = pageSize * pageIndex;
        ByteBuffer index = indexBuffer.duplicate();
        index.position(pagePosition);
        index.limit(pagePosition + pageSize);
        ByteBuffer buffer = index.slice();
        // System.err.printf("Created page %d at position %,d for %,d entries\n", pageIndex,
        // pagePosition, (pageSize - HEADER_SIZE) / ENTRY_SIZE);
        return new Page(buffer, pageIndex);
    }

    // public void pack() {
    // if (sort()) {
    // buffer.force();
    // }
    // }
    //
    // private boolean sort() {
    // synchronized (writeView) {
    // boolean isPacked = isPacked();
    // if (isPacked) {
    // return false;
    // } else {
    // Stopwatch s = Stopwatch.createStarted();
    // EntryList list = asEntryList();
    // Collections.sort(list);
    // System.err.printf("Sorted %,d entries of page %d in %s\n", list.size(), pageIndex,
    // s.stop());
    // setPacked(true);
    // return true;
    // }
    // }
    // }

    // private EntryList entryView;

    public List<Entry> asEntryList() {
        // if (entryView == null) {
        ByteBuffer view = writeView.duplicate();
        view.rewind();
        int numObjects = numObjects();
        view.limit(ENTRY_SIZE * numObjects);
        EntryList list = new EntryList(view);
        return list;
        // entryView = list;
        // }
        // return entryView;
    }

    // private List<byte[]> idView;

    private List<byte[]> asIdList() {
        // if (idView == null) {
        ByteBuffer view = writeView.duplicate();
        view.rewind();
        int numObjects = numObjects();
        view.limit(ENTRY_SIZE * numObjects);
        IdList list = new IdList(view);
        return list;
        // idView = list;
        // }
        // return idView;
    }

    public List<byte[]> findPartial(byte[] partialId) {
        final int size = numObjects();
        for (int i = 0; i < size; i++) {

        }
        return null;
    }

    private static final class EntryList extends AbstractList<Entry> implements RandomAccess {

        private ByteBuffer buffer;

        public EntryList(ByteBuffer buffer) {
            this.buffer = buffer;
        }

        @Override
        public Entry get(int index) {
            int offset = index * ENTRY_SIZE;
            buffer.position(offset);
            Entry entry = Entry.read(buffer);
            return entry;
        }

        @Override
        public Entry set(int index, Entry element) {
            ByteBuffer buffer = this.buffer;
            final int offset = index * ENTRY_SIZE;
            buffer.position(offset);
            Entry.write(buffer, element);
            return null;
        }

        @Override
        public int size() {
            int limit = buffer.limit();
            int size = limit / ENTRY_SIZE;
            return size;
        }

        @Override
        public void add(final int index, final Entry e) {
            final int size = size();
            Preconditions.checkArgument(buffer.capacity() >= (size + 1) * ENTRY_SIZE);
            if (index < size) {
                shift(index);
            }
            int position = index * ENTRY_SIZE;
            buffer.position(position);
            if (buffer.limit() == buffer.position()) {
                buffer.limit(buffer.position() + ENTRY_SIZE);
            }
            Entry.write(buffer, e);
        }

        private void shift(int index) {
            int size = size();
            int from = index * ENTRY_SIZE;
            int to = size * ENTRY_SIZE;
            int limit = buffer.limit();
            buffer.limit(limit + ENTRY_SIZE);

            // for (int i = to - 1; i >= from; i--) {
            // buffer.put(i + ENTRY_SIZE, buffer.get(i));
            // }
            final int length = to - from;
            byte[] buff = getBuffer(length);
            buffer.position(from);
            buffer.get(buff, 0, length);

            buffer.position(from + ENTRY_SIZE);
            buffer.put(buff, 0, length);
        }

        private byte[] getBuffer(final int length) {
            byte[] buff = BUFF.get();
            if (buff == null || buff.length < length) {
                int bufsize = 1024 * (1 + length / 1024);
                buff = new byte[bufsize];
                BUFF.set(buff);
            }
            return buff;
        }
    }

    private static final class IdList extends AbstractList<byte[]> implements RandomAccess {

        private ByteBuffer buffer;

        public IdList(ByteBuffer buffer) {
            this.buffer = buffer;
        }

        @Override
        public byte[] get(int index) {
            int offset = index * ENTRY_SIZE;
            buffer.position(offset);
            byte[] id = new byte[ObjectId.NUM_BYTES];
            buffer.get(id);
            return id;
        }

        @Override
        public byte[] set(int index, byte[] element) {
            throw new UnsupportedOperationException();
        }

        @Override
        public int size() {
            int limit = buffer.limit();
            int size = limit / ENTRY_SIZE;
            return size;
        }

    }

    public static class Entry implements Comparable<Entry> {

        public final byte[] hashCode;

        public int offset;

        private final byte present;

        public int size;

        private Entry(byte[] hashCode, int offset, int size, byte present) {
            this.hashCode = hashCode;
            this.offset = offset;
            this.size = size;
            this.present = present;
        }

        public byte[] getHashCode() {
            return hashCode;
        }

        public int getOffset() {
            return offset;
        }

        @Override
        public int compareTo(Entry o) {
            return ID_ORDER.compare(hashCode, o.hashCode);
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
            return new StringBuilder("Entry[hash: ")
                    .append(ObjectId.createNoClone(getHashCode()).toString()).append(", offset: ")
                    .append(getOffset()).append(']').toString();
        }

        public static void write(ByteBuffer buffer, Entry entry) {
            if (buffer.remaining() < ENTRY_SIZE) {
                throw new BufferOverflowException();
            }
            buffer.put(entry.hashCode);
            buffer.putInt(entry.offset);
            buffer.putInt(entry.size);
            buffer.put(entry.present);
        }

        /**
         * Reads the object id into dst and advances the buffer until the start of the next entry
         */
        public static void readId(byte[] dst, ByteBuffer buffer) {
            Preconditions.checkArgument(dst.length == ObjectId.NUM_BYTES);
            int pos = buffer.position();
            buffer.get(dst);
            buffer.position(pos + ENTRY_SIZE);
        }

        public static Entry read(ByteBuffer buffer) {
            byte[] id = new byte[ObjectId.NUM_BYTES];
            buffer.get(id);
            int offset = buffer.getInt();
            int size = buffer.getInt();
            byte present = buffer.get();
            return new Entry(id, offset, size, present);
        }

    }

    public void pack() {
        if (packed) {
            return;
        }
        List<Entry> entryList = asEntryList();
        // Stopwatch sw = Stopwatch.createStarted();
        Collections.sort(entryList);
        packed = true;
        // System.err.printf("%,d entries sorted in %s\n", entryList.size(), sw.stop());
    }

    public Range<ObjectId> range() {
        return min == null ? Range.singleton(ObjectId.NULL) : Range.closed(
                ObjectId.createNoClone(min), ObjectId.createNoClone(max));
    }

    public Iterator<ObjectId> ids() {
        List<byte[]> asIdList = asIdList();
        return Iterators.transform(asIdList.iterator(), new Function<byte[], ObjectId>() {
            @Override
            public ObjectId apply(byte[] input) {
                return ObjectId.createNoClone(input);
            }
        });
    }

    public void set(List<Entry> sortedEntries) {
        synchronized (writeView) {
            writeView.position(0);
            this.bloomFilter = newBloomFilter();
            this.numObjects.put(0, sortedEntries.size());
            this.min = null;
            this.max = null;
            for (Entry e : sortedEntries) {
                Entry.write(writeView, e);
                byte[] key = e.hashCode.clone();
                bloomFilter.put(key);
                if (min == null) {
                    min = key;
                    max = key;
                } else {
                    min = ID_ORDER.min(key, min);
                    max = ID_ORDER.max(key, max);
                }
            }
            packed = true;
        }
    }
}
