/* Copyright (c) 2012-2014 Boundless and others.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Eclipse Distribution License v1.0
 * which accompanies this distribution, and is available at
 * https://www.eclipse.org/org/documents/edl-v10.html
 *
 * Contributors:
 * Gabriel Roldan (Boundless) - initial implementation
 */
package org.locationtech.geogig.api;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;
import static org.locationtech.geogig.api.RevObject.TYPE.FEATURE;
import static org.locationtech.geogig.api.RevObject.TYPE.TREE;
import static org.locationtech.geogig.api.RevTree.NORMALIZED_SIZE_LIMIT;

import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import javax.annotation.Nullable;

import org.locationtech.geogig.api.RevObject.TYPE;
import org.locationtech.geogig.api.plumbing.HashObject;
import org.locationtech.geogig.repository.SpatialOps;
import org.locationtech.geogig.storage.NodePathStorageOrder;
import org.locationtech.geogig.storage.ObjectDatabase;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;
import com.google.common.base.Stopwatch;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableList.Builder;
import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.PeekingIterator;
import com.vividsolutions.jts.geom.Envelope;

public class RevTreeBuilderNew {

    private static final Logger LOGGER = LoggerFactory.getLogger(RevTreeBuilderNew.class);

    /**
     * How many children nodes to hold before forcing normalization of the internal data structures
     * into tree buckets on the database
     * 
     * @todo make this configurable
     */
    public static final int DEFAULT_NORMALIZATION_THRESHOLD = 1000 * 1000;

    private final NodeIndex nodeIndex;

    protected final TreeMap<Integer, Bucket> bucketTreesByBucket;

    private final ObjectDatabase db;

    private int depth;

    private long initialSize;

    private int initialNumTrees;

    private static NodePathStorageOrder STORAGE_ORDER = new NodePathStorageOrder();

    private Map<ObjectId, RevTree> pendingWritesCache;

    private Platform platform;

    /**
     * Empty tree constructor, used to create trees from scratch
     * 
     * @param db
     * @param serialFactory
     */
    public RevTreeBuilderNew(ObjectDatabase db, Platform platform) {
        this(db, null, platform);
    }

    /**
     * Only useful to {@link #build() build} the named {@link #empty() empty} tree
     */
    private RevTreeBuilderNew() {
        db = null;
        pendingWritesCache = Maps.newTreeMap();
        bucketTreesByBucket = Maps.newTreeMap();
        nodeIndex = null;
    }

    /**
     * Copy constructor with tree depth
     */
    public RevTreeBuilderNew(ObjectDatabase db, @Nullable final RevTree copy, Platform platform) {
        this(db, copy, 0, platform, new TreeMap<ObjectId, RevTree>());
    }

    /**
     * Copy constructor
     */
    private RevTreeBuilderNew(final ObjectDatabase db, @Nullable final RevTree copy,
            final int depth, Platform platform, final Map<ObjectId, RevTree> pendingWritesCache) {

        checkNotNull(db);
        checkNotNull(pendingWritesCache);

        this.db = db;
        this.depth = depth;
        this.platform = platform;
        this.pendingWritesCache = pendingWritesCache;
        this.bucketTreesByBucket = Maps.newTreeMap();

        if (copy != null) {
            this.initialSize = copy.size();
            this.initialNumTrees = copy.numTrees();

            if (copy.trees().isPresent()) {
                checkArgument(!copy.buckets().isPresent());
                for (Node node : copy.trees().get()) {
                    putInternal(node);
                }
            }
            if (copy.features().isPresent()) {
                checkArgument(!copy.buckets().isPresent());
                for (Node node : copy.features().get()) {
                    putInternal(node);
                }
            }
            if (copy.buckets().isPresent()) {
                checkArgument(!copy.features().isPresent());
                bucketTreesByBucket.putAll(copy.buckets().get());
            }
        }
        this.nodeIndex = new FileNodeIndex(platform);
    }

    /**
     */
    private void putInternal(Node node) {
        checkArgument(FEATURE.equals(node.getType()) || TREE.equals(node.getType()),
                "Only tree or feature nodes can be added to a tree: %s", node);
        nodeIndex.add(node);
    }

    private RevTree loadTree(final ObjectId subtreeId) {
        RevTree subtree = this.pendingWritesCache.get(subtreeId);
        if (subtree == null) {
            subtree = db.getTree(subtreeId);
        }
        return subtree;
    }

    private long sizeOfTree(ObjectId treeId) {
        if (treeId.isNull()) {
            return 0L;
        }
        RevTree tree = loadTree(treeId);
        return tree.size();
    }

    /**
     * Splits the cached entries into subtrees and saves them, making sure the tree contains either
     * only entries or subtrees
     */
    private RevTree normalize() {
        Stopwatch sw = Stopwatch.createStarted();
        RevTree unnamedTree;

        final long numPendingChanges = nodeIndex.size();
        if (bucketTreesByBucket.isEmpty() && numPendingChanges <= NORMALIZED_SIZE_LIMIT) {
            unnamedTree = normalizeToChildren();
        } else {
            unnamedTree = normalizeToBuckets();
            checkState(!unnamedTree.features().isPresent());
            checkState(!unnamedTree.trees().isPresent());

            if (unnamedTree.size() <= NORMALIZED_SIZE_LIMIT) {
                this.bucketTreesByBucket.clear();
                if (unnamedTree.buckets().isPresent()) {
                    unnamedTree = moveBucketsToChildren(unnamedTree);
                }
                if (this.depth == 0) {
                    pendingWritesCache.clear();
                }
            }
        }

        final int pendingWritesThreshold = 10 * 1000;
        final boolean topLevelTree = this.depth == 0;// am I an actual (addressable) tree or bucket
                                                     // tree of a higher level one?
        final boolean forceWrite = pendingWritesCache.size() >= pendingWritesThreshold;
        if (!pendingWritesCache.isEmpty() && (topLevelTree || forceWrite)) {
            LOGGER.debug("calling db.putAll for {} buckets because {}...", pendingWritesCache
                    .size(), (topLevelTree ? "writing top level tree" : "there are "
                    + pendingWritesCache.size() + " pending bucket writes"));
            Stopwatch sw2 = Stopwatch.createStarted();
            db.putAll(pendingWritesCache.values().iterator());
            pendingWritesCache.clear();
            LOGGER.debug("done in {}", sw2.stop());
        }
        this.initialSize = unnamedTree.size();
        this.initialNumTrees = unnamedTree.numTrees();
        if (this.depth == 0) {
            LOGGER.debug("Normalization took {}. Changes: {}", sw.stop(), numPendingChanges);
        }
        return unnamedTree;
    }

    /**
     * @param tree
     * @return
     */
    private RevTree moveBucketsToChildren(RevTree tree) {
        checkState(tree.buckets().isPresent());
        checkState(this.bucketTreesByBucket.isEmpty());

        for (Bucket bucket : tree.buckets().get().values()) {
            ObjectId id = bucket.id();
            RevTree bucketTree = this.loadTree(id);
            if (bucketTree.buckets().isPresent()) {
                moveBucketsToChildren(bucketTree);
            } else {
                Iterator<Node> children = bucketTree.children();
                while (children.hasNext()) {
                    Node next = children.next();
                    putInternal(next);
                }
            }
        }

        return normalizeToChildren();
    }

    /**
     * 
     */
    private RevTree normalizeToChildren() {
        Preconditions.checkState(this.bucketTreesByBucket.isEmpty());
        Iterator<Node> nodes = nodeIndex.nodes();
        Builder<Node> features = ImmutableList.builder();
        Builder<Node> trees = ImmutableList.builder();
        long size = 0;
        while (nodes.hasNext()) {
            Node node = nodes.next();
            // ignore delete requests, we're building a leaf tree out of our nodes
            if (isDelete(node)) {
                continue;
            }
            if (FEATURE.equals(node.getType())) {
                features.add(node);
                size++;
            } else {
                trees.add(node);
                size += sizeOf(node);
            }
        }
        ImmutableList<Node> featureNodes = features.build();
        ImmutableList<Node> treeNodes = trees.build();
        RevTreeImpl unnamedTree = RevTreeImpl.createLeafTree(ObjectId.NULL, size, featureNodes,
                treeNodes);
        return unnamedTree;
    }

    private boolean isDelete(Node node) {
        return ObjectId.NULL.equals(node.getObjectId());
    }

    private long sizeOf(Node node) {
        return node.getType().equals(TYPE.TREE) ? sizeOfTree(node.getObjectId()) : 1L;
    }

    /**
     * @return
     * 
     */
    private RevTree normalizeToBuckets() {
        // aggregate size delta for all changed buckets
        long sizeDelta = 0L;
        // aggregate number of trees delta for all changed buckets
        int treesDelta = 0;

        try {
            final Map<Integer, RevTree> bucketTrees = getBucketTrees();

            PeekingIterator<Node> nodes = Iterators.peekingIterator(nodeIndex.nodes());

            List<RevTree> newLeafTreesToSave = Lists.newArrayList();

            while (nodes.hasNext()) {
                Node peek = nodes.peek();
                final Integer bucketIndex = computeBucket(peek.getName());
                final RevTree currentBucketTree = bucketTrees.get(bucketIndex);
                final int bucketDepth = this.depth + 1;

                final RevTreeBuilderNew bucketTreeBuilder;
                bucketTreeBuilder = new RevTreeBuilderNew(this.db, currentBucketTree, bucketDepth,
                        this.platform, this.pendingWritesCache);
                do {
                    Node next = nodes.next();
                    //bucketTreeBuilder.put(next);
                } while (nodes.hasNext()
                        && computeBucket(nodes.peek().getName()).equals(bucketIndex));

                final RevTree modifiedBucketTree = bucketTreeBuilder.build();
                final long bucketSizeDelta = modifiedBucketTree.size() - currentBucketTree.size();
                final int bucketTreesDelta = modifiedBucketTree.numTrees()
                        - currentBucketTree.numTrees();
                sizeDelta += bucketSizeDelta;
                treesDelta += bucketTreesDelta;
                if (modifiedBucketTree.isEmpty()) {
                    bucketTreesByBucket.remove(bucketIndex);
                } else {
                    final Bucket currBucket = this.bucketTreesByBucket.get(bucketIndex);
                    if (currBucket == null || !currBucket.id().equals(modifiedBucketTree.getId())) {
                        // if (currBucket != null) {
                        // db.delete(currBucket.id());
                        // }
                        // have it on the pending writes set only if its not a leaf tree. Non bucket
                        // trees may be too large and cause OOM
                        if (null != pendingWritesCache.remove(currentBucketTree.getId())) {
                            // System.err.printf(" ---> removed bucket %s from list\n",
                            // currentBucketTree.getId());
                        }
                        if (modifiedBucketTree.buckets().isPresent()) {
                            pendingWritesCache.put(modifiedBucketTree.getId(), modifiedBucketTree);
                        } else {
                            // db.put(modifiedBucketTree);
                            newLeafTreesToSave.add(modifiedBucketTree);
                        }
                        Envelope bucketBounds = SpatialOps.boundsOf(modifiedBucketTree);
                        Bucket bucket = Bucket.create(modifiedBucketTree.getId(), bucketBounds);
                        bucketTreesByBucket.put(bucketIndex, bucket);
                    }
                }
            }

            if (!newLeafTreesToSave.isEmpty()) {
                db.putAll(newLeafTreesToSave.iterator());
                newLeafTreesToSave.clear();
                newLeafTreesToSave = null;
            }
        } catch (RuntimeException e) {
            throw e;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

        // compute final size and number of trees out of the aggregate deltas
        long accSize = sizeDelta;
        if (initialSize > RevTree.NORMALIZED_SIZE_LIMIT) {
            accSize += initialSize;
        }
        int accChildTreeCount = this.initialNumTrees + treesDelta;

        RevTreeImpl unnamedTree;
        unnamedTree = RevTreeImpl.createNodeTree(ObjectId.NULL, accSize, accChildTreeCount,
                this.bucketTreesByBucket);
        return unnamedTree;
    }

    private Map<Integer, RevTree> getBucketTrees() {
        Map<Integer, RevTree> bucketTrees = new HashMap<>();
        final Map<ObjectId, Integer> missing = Maps.newHashMap();
        for (int bucketIndex = 0; bucketIndex < RevTree.MAX_BUCKETS; bucketIndex++) {
            Bucket bucket = bucketTreesByBucket.get(bucketIndex);
            RevTree cached = bucket == null ? RevTree.EMPTY : pendingWritesCache.get(bucket.id());
            if (cached == null) {
                missing.put(bucket.id(), Integer.valueOf(bucketIndex));
            } else {
                bucketTrees.put(Integer.valueOf(bucketIndex), cached);
            }
        }
        if (!missing.isEmpty()) {
            Iterator<RevObject> all = db.getAll(missing.keySet());
            while (all.hasNext()) {
                RevObject next = all.next();
                Integer bucketIndex = missing.get(next.getId());
                bucketTrees.put(bucketIndex, (RevTree) next);
            }
        }
        return bucketTrees;
    }

    protected final Integer computeBucket(final String path) {
        return STORAGE_ORDER.bucket(path, this.depth);
    }

    /**
     * Adds or replaces an element in the tree with the given key.
     * 
     * @param key non null
     * @param value non null
     */
    public RevTreeBuilderNew put(final Node node) {
        Preconditions.checkNotNull(node, "node can't be null");

        putInternal(node);

        return this;
    }

    /**
     * Removes an element from the tree
     * 
     * @param childName the name of the child to remove
     * @return {@code this}
     */
    public RevTreeBuilderNew remove(final String childName) {
        Preconditions.checkNotNull(childName, "key can't be null");
        nodeIndex.add(Node.create(childName, ObjectId.NULL, ObjectId.NULL, FEATURE, null));
        return this;
    }

    /**
     * @return the new tree, not saved to the object database. Any bucket tree though is saved when
     *         this method returns.
     */
    public RevTree build() {
        RevTree unnamedTree;
        try {
            unnamedTree = normalize();
        } finally {
            if (nodeIndex != null) {
                nodeIndex.close();
            }
        }
        final boolean empty = unnamedTree.isEmpty();
        final boolean buckets = unnamedTree.buckets().isPresent();
        final boolean leafTree = unnamedTree.features().isPresent()
                || unnamedTree.trees().isPresent();
        checkState(empty || (buckets != leafTree));

        ObjectId treeId = new HashObject().setObject(unnamedTree).call();
        RevTreeImpl namedTree = RevTreeImpl.create(treeId, unnamedTree.size(), unnamedTree);
        return namedTree;
    }

    /**
     * Deletes all nodes that represent subtrees
     * 
     * @return {@code this}
     */
    // public RevTreeBuilderNew clearSubtrees() {
    // this.treeChanges.clear();
    // return this;
    // }

    /**
     * @return a new instance of a properly "named" empty tree (as in with a proper object id after
     *         applying {@link HashObject})
     */
    public static RevTree empty() {
        RevTree theEmptyTree = new RevTreeBuilderNew().build();
        return theEmptyTree;
    }
}
