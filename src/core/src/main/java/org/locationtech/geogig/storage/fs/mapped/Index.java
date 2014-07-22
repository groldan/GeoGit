package org.locationtech.geogig.storage.fs.mapped;

import static com.google.common.base.Preconditions.checkState;
import static java.nio.file.StandardOpenOption.READ;
import static java.nio.file.StandardOpenOption.WRITE;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileChannel.MapMode;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.locationtech.geogig.api.ObjectId;
import org.locationtech.geogig.storage.fs.mapped.Page.Entry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Function;
import com.google.common.base.Stopwatch;
import com.google.common.base.Throwables;
import com.google.common.collect.Iterables;
import com.google.common.collect.Iterators;
import com.google.common.collect.Range;
import com.google.common.collect.RangeMap;
import com.google.common.collect.TreeRangeMap;
import com.google.common.collect.UnmodifiableIterator;
import com.google.common.hash.BloomFilter;
import com.google.common.hash.Funnels;
import com.google.common.io.Files;

public class Index {

    private static final Logger LOGGER = LoggerFactory.getLogger(Index.class);

    private static final int MAX_ENTRIES_PER_PAGE = 100_000;

    private static final int PAGE_SIZE = Page.HEADER_SIZE + Page.ENTRY_SIZE * MAX_ENTRIES_PER_PAGE;

    private static final int MAX_DATA_SIZE = 512 * 1024 * 1024;

    private MappedByteBuffer indexBuffer;

    private File indexFile;

    private File dataFile;

    private FileChannel indexChannel;

    private FileChannel dataWriteChannel, dataReadChannel;

    private ArrayList<Page> pages;

    private volatile boolean open;

    private BloomFilter<byte[]> bloomFilter;

    public Index(final File indexFile) {
        this.indexFile = indexFile;
        this.dataFile = new File(indexFile.getParentFile(), Files.getNameWithoutExtension(indexFile
                .getName()) + ".gig");
        this.pages = new ArrayList<>();
        try {
            checkState(indexFile.exists() && indexFile.isFile() || indexFile.createNewFile());
            checkState(dataFile.exists() && dataFile.isFile() || dataFile.createNewFile());
        } catch (IOException e) {
            throw Throwables.propagate(e);
        }
        // System.err.println("Created index " + indexFile);
    }

    private void checkOpen() {
        if (open) {
            return;
        }
        open();
    }

    private synchronized void open() {
        if (open) {
            return;
        }
        try {
            indexChannel = (FileChannel) java.nio.file.Files.newByteChannel(indexFile.toPath(),
                    READ, WRITE);

            indexBuffer = indexChannel.map(MapMode.READ_WRITE, 0, 128 * 1024 * 1024);
            indexBuffer.order(ByteOrder.BIG_ENDIAN);

            dataWriteChannel = (FileChannel) java.nio.file.Files.newByteChannel(dataFile.toPath(),
                    StandardOpenOption.APPEND);

            dataReadChannel = (FileChannel) java.nio.file.Files.newByteChannel(dataFile.toPath(),
                    READ);
            loadPages();
            bloomFilter = loadBloomFilter();
            open = true;
        } catch (IOException e) {
            throw Throwables.propagate(e);
        }
    }

    private void loadPages() {
        Page page;
        while (true) {
            page = newPage();
            pagesRangeMap.put(page.range(), page);
            if (page.numObjects() < MAX_ENTRIES_PER_PAGE) {
                break;
            }
        }
    }

    public synchronized void close() {
        if (!open) {
            return;
        }
        try {
            currentPage().pack();
            sortPages();
            dataWriteChannel.force(false);

            dataWriteChannel.close();
            indexBuffer.force();
            indexChannel.force(false);
            indexChannel.close();
            indexBuffer = null;

            // serialize bloom filter
            saveBloomFilter(bloomFilter);
            open = false;
        } catch (IOException e) {
            LOGGER.warn("Error closing index {}", indexFile, e);
        }
    }

    private void sortPages() {
        List<Page> pages = pages();
        Iterable<Entry> entries = Iterables.concat(Iterables.transform(pages,
                new Function<Page, List<Page.Entry>>() {
                    @Override
                    public List<Entry> apply(Page input) {
                        return input.asEntryList();
                    }
                }));
        Stopwatch s = Stopwatch.createStarted();
        Entry[] array = Iterables.toArray(entries, Entry.class);
        // System.err.printf("Array of %,d entries created in %s\n", array.length, s.stop());
        // s.reset().start();
        Arrays.sort(array);
        // System.err.printf("Array of %,d entries sorted in %s\n", array.length, s.stop());

        UnmodifiableIterator<List<Entry>> sortedPartitions = Iterators.partition(
                Iterators.forArray(array), MAX_ENTRIES_PER_PAGE);
        Iterator<Page> pagesIterator = pages().iterator();
        while (sortedPartitions.hasNext()) {
            List<Entry> sortedEntries = sortedPartitions.next();
            Page page = pagesIterator.next();
            page.set(sortedEntries);
        }
    }

    private File bloomFilterFile() {
        File bloomFile = new File(dataFile.getParentFile(), Files.getNameWithoutExtension(dataFile
                .getName()) + ".bloom");
        return bloomFile;
    }

    private void saveBloomFilter(BloomFilter<byte[]> bloomFilter) throws IOException {
        File bloomFile = bloomFilterFile();
        bloomFile.createNewFile();
        ObjectOutputStream oo = new ObjectOutputStream(new FileOutputStream(bloomFile));
        oo.writeObject(bloomFilter);
        oo.flush();
        oo.close();
    }

    private BloomFilter<byte[]> loadBloomFilter() {
        File bloomFile = bloomFilterFile();
        BloomFilter<byte[]> bloomf;
        if (bloomFile.exists()) {
            try (FileInputStream in = new FileInputStream(bloomFile)) {
                bloomf = (BloomFilter<byte[]>) new ObjectInputStream(in).readObject();
            } catch (Exception e) {
                throw Throwables.propagate(e);
            }
        } else {
            bloomf = BloomFilter.create(Funnels.byteArrayFunnel(), 1_000_000, 0.01);
        }
        return bloomf;
    }

    private List<Page> pages() {
        return pages;
    }

    private Page currentPage() {
        if (pages.isEmpty()) {
            newPage();
        }
        return pages.get(pages.size() - 1);
    }

    private Page newPage() {
        final int pageN = pages.size();
        Page page;
        try {
            if (pageN > 0) {
                currentPage().pack();
            }
            // page = Page.create(indexChannel, pageN, PAGE_SIZE);
            page = Page.create(indexBuffer, pageN, PAGE_SIZE);
        } catch (IOException e) {
            throw Throwables.propagate(e);
        }
        pages.add(page);
        return page;
    }

    public synchronized boolean add(byte[] key, ByteData data) {
        checkOpen();
        // if (contains(key)) {
        // return false;
        // }
        int offset;
        // synchronized (dataChannel) {
        final int size = data.size();
        try {
            // offset = (int) dataWriteChannel.size();
            offset = (int) dataWriteChannel.position();
            if (size + offset > MAX_DATA_SIZE) {
                return false;
            }
            // dataWriteChannel.position(offset);
            ByteBuffer src = ByteBuffer.wrap(data.bytes(), 0, size);
            dataWriteChannel.write(src);
            Page page = currentPage();

            // Range<ObjectId> preRange = page.range();

            Result result = page.add(key, offset, size);
            switch (result) {
            case KEY_EXISTS:
                return false;
            case PAGE_FULL:
                page = newPage();
                return add(key, data);
            case SUCCESS:
                // Range<ObjectId> postRange = page.range();
                // if (!preRange.equals(postRange)) {
                // pagesRangeMap.remove(preRange);
                // pagesRangeMap.put(postRange, page);
                // }
                break;
            }
            bloomFilter.put(key);
        } catch (IOException e) {
            e.printStackTrace();
            throw Throwables.propagate(e);
        }
        return true;
        // }
    }

    public synchronized boolean delete(byte[] key) {
        checkOpen();
        Iterable<Page> pages = pages();
        boolean deleted = false;
        for (Page page : pages) {
            deleted |= page.delete(key);
        }
        return deleted;
    }

    private final RangeMap<ObjectId, Page> pagesRangeMap = TreeRangeMap.create();

    public boolean find(byte[] key, ByteData data) {
        checkOpen();

        // final ObjectId id = ObjectId.createNoClone(key);
        // int pagesTraversed = 0;
        // RangeMap<ObjectId, Page> mightContainPages =
        // pagesRangeMap.subRangeMap(Range.singleton(id));
        //
        // Page.Entry entry = null;
        // Map<Range<ObjectId>, Page> mapOfRanges = mightContainPages.asMapOfRanges();
        // for (Page page : mapOfRanges.values()) {
        // pagesTraversed++;
        // entry = page.find(key);
        // if (entry != null) {
        // read(data, entry.offset, entry.size);
        // break;
        // }
        // }
        // // System.err.println("pagesTraversed: " + pagesTraversed);
        // return entry != null;

        int pagesTraversed = 0;
        List<Page> pages = pages();
        try {
            for (int i = 0; i < pages.size(); i++) {
                Page page = pages.get(i);
                pagesTraversed++;
                Page.Entry entry = page.find(key);
                if (entry != null) {
                    read(data, entry.offset, entry.size);
                    return true;
                }
            }
            return false;
        } finally {
            // System.err.println("pagesTraversed: " + pagesTraversed);
        }
    }

    private void read(ByteData data, final int offset, final int size) {
        synchronized (dataReadChannel) {
            try {
                dataReadChannel.position(offset);
                data.reset();
                ByteBuffer dst = data.asByteBuffer(size);
                dataReadChannel.read(dst, offset);
            } catch (IOException e) {
                System.err.printf("Error reading %s at position %,d, size %,d", dataFile, offset,
                        size);
                e.printStackTrace();
                throw Throwables.propagate(e);
            }
        }
    }

    public boolean contains(byte[] key) {
        checkOpen();
        // if (!bloomFilter.mightContain(key)) {
        // return false;
        // }
        List<Page> pages = pages();
        for (int i = 0; i < pages.size(); i++) {
            Page page = pages.get(i);
            if (page.indexOf(key) > -1) {
                return true;
            }
        }
        return false;
    }

    public List<byte[]> findPartial(byte[] partialId) {
        checkOpen();
        Iterable<Page> pages = pages();
        List<byte[]> matches = new ArrayList<>();
        for (Page page : pages) {
            List<byte[]> idMatches = page.findPartial(partialId);
            matches.addAll(idMatches);
        }
        return matches;
    }
}
