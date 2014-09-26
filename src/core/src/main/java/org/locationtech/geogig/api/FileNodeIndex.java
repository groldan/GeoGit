/* Copyright (c) 2014 Boundless and others.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Eclipse Distribution License v1.0
 * which accompanies this distribution, and is available at
 * https://www.eclipse.org/org/documents/edl-v10.html
 *
 * Contributors:
 * Gabriel Roldan (Boundless) - initial implementation
 */
package org.locationtech.geogig.api;

import static com.google.common.base.Preconditions.checkState;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.ByteArrayOutputStream;
import java.io.Closeable;
import java.io.DataInputStream;
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Random;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.concurrent.Future;

import org.locationtech.geogig.storage.NodePathStorageOrder;
import org.locationtech.geogig.storage.NodeStorageOrder;
import org.locationtech.geogig.storage.datastream.FormatCommonV2;

import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import com.google.common.collect.AbstractIterator;
import com.google.common.collect.Iterators;
import com.google.common.collect.UnmodifiableIterator;
import com.google.common.io.Closeables;
import com.google.common.util.concurrent.Futures;
import com.ning.compress.lzf.LZFInputStream;
import com.ning.compress.lzf.LZFOutputStream;

public class FileNodeIndex implements Closeable, NodeIndex {

    private static final NodeStorageOrder NODE_COMPARATOR = new NodeStorageOrder();

    private static final int PARTITION_SIZE = 1000_000;

    private static final class IndexPartition {

        private static final NodePathStorageOrder COMPARATOR = new NodePathStorageOrder();

        // use a TreeMap instead of a TreeSet to account for the rare case of a hash collision
        private SortedMap<String, Node> cache = new TreeMap<>(COMPARATOR);

        private File tmpFolder;

        public IndexPartition(final File tmpFolder) {
            this.tmpFolder = tmpFolder;
        }

        public void add(Node node) {
            cache.put(node.getName(), node);
        }

        public Iterator<Node> getSortedNodes() {
            return cache.values().iterator();
        }

        public File flush() {
            Iterator<Node> nodes = getSortedNodes();
            final File file;
            try {
                file = File.createTempFile("geogigNodes", ".idx", tmpFolder);
                file.deleteOnExit();
                // System.err.println("Created index file " + file.getAbsolutePath());
                FastByteArrayOutputStream buf = new FastByteArrayOutputStream();

                OutputStream fileOut = new BufferedOutputStream(new FileOutputStream(file),
                        1024 * 16);
                fileOut = new LZFOutputStream(fileOut);
                try {
                    while (nodes.hasNext()) {
                        Node node = nodes.next();
                        buf.reset();
                        DataOutput out = new DataOutputStream(buf);
                        try {
                            FormatCommonV2.writeNode(node, out);
                        } catch (IOException e) {
                            throw Throwables.propagate(e);
                        }
                        int size = buf.size();
                        fileOut.write(buf.bytes(), 0, size);
                    }
                } finally {
                    this.cache.clear();
                    this.cache = null;
                    fileOut.close();
                }
            } catch (Exception e) {
                e.printStackTrace();
                throw Throwables.propagate(e);
            }
            return file;
        }
    }

    private static final Random random = new Random();

    private IndexPartition currPartition;

    private List<Future<File>> indexFiles = new LinkedList<Future<File>>();

    private List<CompositeNodeIterator> openIterators = new LinkedList<CompositeNodeIterator>();

    private File tmpFolder;

    private static final int MAX_DEPTH_TRACKED_SIZE = 8;

    private LevelSizeMatrix sizeMatrix = new LevelSizeMatrix(RevTree.MAX_BUCKETS,
            MAX_DEPTH_TRACKED_SIZE);

    public FileNodeIndex(Platform platform) {
        File tmpFolder = new File(platform.getTempDir(), "nodeindex" + Math.abs(random.nextInt()));
        checkState(tmpFolder.mkdirs());
        this.tmpFolder = tmpFolder;
        this.currPartition = new IndexPartition(this.tmpFolder);
    }

    @Override
    public void close() {
        try {
            for (CompositeNodeIterator it : openIterators) {
                it.close();
            }
            for (Future<File> ff : indexFiles) {
                try {
                    File file = ff.get();
                    file.delete();
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        } finally {
            tmpFolder.delete();
            openIterators.clear();
            indexFiles.clear();
        }
    }

    private long size;

    public long size() {
        return size;
    }

    @Override
    public synchronized void add(Node node) {
        currPartition.add(node);
        if (currPartition.cache.size() == PARTITION_SIZE) {
            flush(currPartition);
            currPartition = new IndexPartition(this.tmpFolder);
        }
        this.size++;
        List<Integer> bucketsByDepth = NODE_COMPARATOR.bucketsByDepth(node);
        for (int depth = 0; depth < MAX_DEPTH_TRACKED_SIZE; depth++) {
            int bucketIndex = bucketsByDepth.get(depth).intValue();
            sizeMatrix.add(bucketIndex, depth);
        }
    }

    private void flush(final IndexPartition ip) {
        File flush = ip.flush();
        indexFiles.add(Futures.immediateCheckedFuture(flush));
        // indexFiles.add(executorService.submit(new Callable<File>() {
        //
        // @Override
        // public File call() throws Exception {
        // return ip.flush();
        // }
        // }));
    }

    @Override
    public Iterator<Node> nodes() {
        System.err.print(sizeMatrix);
        sizeMatrix.validate(size);

        List<File> files = new ArrayList<File>(indexFiles.size());
        try {
            for (Future<File> ff : indexFiles) {
                files.add(ff.get());
            }
        } catch (Exception e) {
            e.printStackTrace();
            throw Throwables.propagate(Throwables.getRootCause(e));
        }

        List<Node> cached = new ArrayList<>(currPartition.cache.size());
        cached.addAll(currPartition.cache.values());
        currPartition.cache.clear();
        return new CompositeNodeIterator(files, cached.iterator());
    }

    private static class CompositeNodeIterator extends AbstractIterator<Node> {

        private NodeStorageOrder order = new NodeStorageOrder();

        private List<IndexIterator> openIterators;

        private UnmodifiableIterator<Node> delegate;

        public CompositeNodeIterator(List<File> files, Iterator<Node> unflushedAndSorted) {

            openIterators = new ArrayList<IndexIterator>();
            LinkedList<Iterator<Node>> iterators = new LinkedList<Iterator<Node>>();
            for (File f : files) {
                IndexIterator iterator = new IndexIterator(f);
                openIterators.add(iterator);
                iterators.add(iterator);
            }
            if (unflushedAndSorted.hasNext()) {
                iterators.add(unflushedAndSorted);
            }
            delegate = Iterators.mergeSorted(iterators, order);
        }

        public void close() {
            for (IndexIterator it : openIterators) {
                it.close();
            }
            openIterators.clear();
        }

        @Override
        protected Node computeNext() {
            if (delegate.hasNext()) {
                return delegate.next();
            }
            return endOfData();
        }

    }

    private static class IndexIterator extends AbstractIterator<Node> {

        private DataInputStream in;

        public IndexIterator(File file) {
            Preconditions.checkArgument(file.exists(), "file %s does not exist", file);
            try {
                if (this.in == null) {
                    InputStream fin = new BufferedInputStream(new FileInputStream(file), 64 * 1024);
                    fin = new LZFInputStream(fin);
                    this.in = new DataInputStream(fin);
                }

            } catch (IOException e) {
                throw Throwables.propagate(e);
            }
        }

        public void close() {
            Closeables.closeQuietly(in);
        }

        @Override
        protected Node computeNext() {
            try {
                Node node = FormatCommonV2.readNode(in);
                return node;
            } catch (EOFException eof) {
                Closeables.closeQuietly(in);
                return endOfData();
            } catch (Exception e) {
                Closeables.closeQuietly(in);
                throw Throwables.propagate(e);
            }
        }

    }

    private static class FastByteArrayOutputStream extends ByteArrayOutputStream {

        public FastByteArrayOutputStream() {
            super(16 * 1024);
        }

        public int size() {
            return super.count;
        }

        public byte[] bytes() {
            return super.buf;
        }
    }

    /**
     * Like an adjacency matrix but tracks number of nodes per bucket depth and bucket index
     */
    private static class LevelSizeMatrix {

        private int[][] matrix;

        public LevelSizeMatrix(final int maxBuckets, final int maxDepth) {
            matrix = new int[maxBuckets][maxDepth];
        }

        public void validate(long size) {
            final int maxBuckets = matrix.length;
            final int maxDepth = matrix[0].length;
            for (int depth = 0; depth < maxDepth; depth++) {
                long depthSize = 0;
                for (int index = 0; index < maxBuckets; index++) {
                    depthSize += matrix[index][depth];
                }
                Preconditions.checkState(depthSize == size);
            }
        }

        public void add(final int bucketIndex, final int bucketDepth) {
            int size = matrix[bucketIndex][bucketDepth];
            matrix[bucketIndex][bucketDepth] = size + 1;
        }

        public int getSize(final int bucketIndex, final int bucketDepth) {
            int size = matrix[bucketIndex][bucketDepth];
            return size;
        }

        public String toString() {
            final int maxBuckets = matrix.length;
            final int maxDepth = matrix[0].length;

            StringBuilder sb = new StringBuilder();
            for (int i = 0; i < maxBuckets; i++) {
                for (int j = 0; j < maxDepth; j++) {
                    sb.append(matrix[i][j]);
                    if (j == maxDepth - 1) {
                        break;
                    }
                    sb.append(", ");
                }
                sb.append('\n');
            }
            return sb.toString();
        }
    }
}
