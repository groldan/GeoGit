/* Copyright (c) 2013 OpenPlans. All rights reserved.
 * This code is licensed under the BSD New License, available at the root
 * application directory.
 */
package org.geogit.api.plumbing.diff;

import java.util.Iterator;
import java.util.List;
import java.util.Map;

import javax.annotation.Nonnull;

import org.geogit.api.NodeRef;
import org.geogit.api.ObjectId;
import org.geogit.api.RevFeatureType;
import org.geogit.api.RevTree;
import org.geogit.api.plumbing.diff.DepthTreeIterator.Strategy;
import org.geogit.repository.SpatialOps;
import org.geogit.storage.ObjectDatabase;
import org.geotools.geometry.jts.ReferencedEnvelope;
import org.geotools.referencing.crs.DefaultGeographicCRS;
import org.opengis.referencing.crs.CoordinateReferenceSystem;

import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.google.common.base.Stopwatch;
import com.google.common.base.Supplier;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.MapDifference;
import com.google.common.collect.MapDifference.ValueDifference;
import com.google.common.collect.Maps;
import com.vividsolutions.jts.geom.Envelope;

/**
 *
 */
public class DiffBoundsCalculator implements Supplier<ReferencedEnvelope> {

    @Nonnull
    private final RevTree fromRootTree;

    @Nonnull
    private final RevTree toRootTree;

    @Nonnull
    private ObjectDatabase objectDb;

    private CoordinateReferenceSystem crs = DefaultGeographicCRS.WGS84;

    private final List<String> pathFilters;

    private Map<ObjectId, CoordinateReferenceSystem> crsCache = Maps.newHashMap();

    public DiffBoundsCalculator(final ObjectDatabase db, final RevTree fromRootTree,
            final RevTree toRootTree) {
        Preconditions.checkNotNull(db);
        Preconditions.checkNotNull(fromRootTree);
        Preconditions.checkNotNull(toRootTree);
        this.objectDb = db;
        this.fromRootTree = fromRootTree;
        this.toRootTree = toRootTree;
        this.pathFilters = Lists.newArrayListWithCapacity(2);
    }

    public void setPathFilters(List<String> paths) {
        pathFilters.clear();
        if (paths != null) {
            pathFilters.addAll(paths);
        }
    }

    @Override
    public ReferencedEnvelope get() {

        final RevTree leftTree = this.fromRootTree;
        final RevTree rightTree = this.toRootTree;

        if (leftTree.getId().equals(rightTree.getId())) {
            return ReferencedEnvelope.create(crs);
        } else if (rightTree.isEmpty()) {
            return boundsOf(leftTree);
        } else if (leftTree.isEmpty()) {
            return boundsOf(rightTree);
        }

        // first, find out which whole child trees are missing in one or another root
        final Map<String, NodeRef> leftSubtrees = listSubtrees(leftTree);
        final Map<String, NodeRef> rightSubtrees = listSubtrees(rightTree);
        final MapDifference<String, NodeRef> changedTrees = Maps.difference(leftSubtrees,
                rightSubtrees);

        ReferencedEnvelope result = ReferencedEnvelope.create(crs);

        final Map<String, NodeRef> entriesOnlyOnLeft = changedTrees.entriesOnlyOnLeft();
        final Map<String, NodeRef> entriesOnlyOnRight = changedTrees.entriesOnlyOnRight();
        expandToInclude(result, entriesOnlyOnLeft.values());
        expandToInclude(result, entriesOnlyOnRight.values());

        Map<String, ValueDifference<NodeRef>> subtreesDiffering = changedTrees.entriesDiffering();
        for (ValueDifference<NodeRef> valueDiff : subtreesDiffering.values()) {
            NodeRef leftValue = valueDiff.leftValue();
            NodeRef rightValue = valueDiff.rightValue();
            if (leftValue == null || rightValue == null) {
                // already taken care of by treating entriesOnlyOnLeft and entriesOnlyOnRight
                continue;
            }
            RevTree leftSubTree = getTree(leftValue.objectId());
            RevTree rightSubTree = getTree(rightValue.objectId());
            Iterator<DiffEntry> subtreeDiffs = new DiffTreeWalk(objectDb, leftSubTree, rightSubTree)
                    .get();
            while (subtreeDiffs.hasNext()) {
                DiffEntry entry = subtreeDiffs.next();
                expandToInclude(result, entry);
            }
        }

        return result;
    }

    private void expandToInclude(ReferencedEnvelope result, DiffEntry entry) {
        NodeRef oldObject = entry.getOldObject();
        NodeRef newObject = entry.getNewObject();
        expandToInclude(result, Lists.newArrayList(oldObject, newObject));
    }

    private RevTree getTree(ObjectId treeId) {
        if (treeId.isNull()) {
            return RevTree.EMPTY;
        }
        RevTree tree = objectDb.get(treeId, RevTree.class);
        return tree;
    }

    private void expandToInclude(ReferencedEnvelope result, Iterable</* nullable */NodeRef> refs) {

        for (NodeRef ref : refs) {
            if (ref == null) {
                continue;
            }
            ObjectId metadataId = ref.getMetadataId();
            CoordinateReferenceSystem origCrs = getOrigCrs(metadataId);
            Envelope env = new Envelope();
            ref.expand(env);
            expandToInclude(result, env, origCrs);
        }
    }

    private CoordinateReferenceSystem getOrigCrs(final ObjectId metadataId) {

        if (metadataId.isNull()) {
            return this.crs;
        }
        CoordinateReferenceSystem crs = crsCache.get(metadataId);
        if (crs == null) {
            Stopwatch sw = new Stopwatch().start();
            RevFeatureType featureType = objectDb.getFeatureType(metadataId);
            crs = featureType.type().getCoordinateReferenceSystem();
            sw.stop();
            crsCache.put(metadataId, crs);
            System.err.printf("Got ft CRS %s in %s\n", metadataId, sw);
        }
        return crs;
    }

    private void expandToInclude(ReferencedEnvelope target, final Envelope env,
            final CoordinateReferenceSystem origCrs) {

        CoordinateReferenceSystem targetCrs = target.getCoordinateReferenceSystem();

        // if (CRS.equalsIgnoreMetadata(targetCrs, origCrs)) {
        // target.expandToInclude(env);
        // } else {
        boolean lenient = true;
        try {
            ReferencedEnvelope transform = ReferencedEnvelope.create(env, origCrs).transform(
                    targetCrs, lenient);
            target.expandToInclude(transform);
        } catch (Exception e) {
            throw Throwables.propagate(e);
        }
        // }
    }

    private Map<String, NodeRef> listSubtrees(RevTree oldTree) {
        ObjectId metadataId = ObjectId.NULL;
        Iterator<NodeRef> subtrees = new DepthTreeIterator("", metadataId, oldTree, objectDb,
                Strategy.RECURSIVE_TREES_ONLY);
        Function<NodeRef, String> keyFunction = new Function<NodeRef, String>() {

            @Override
            public String apply(NodeRef input) {
                return input.path();
            }
        };
        ImmutableMap<String, NodeRef> subtreesByPath = Maps.uniqueIndex(subtrees, keyFunction);
        return subtreesByPath;
    }

    private ReferencedEnvelope boundsOf(RevTree tree) {
        Envelope nativeBounds = SpatialOps.boundsOf(tree);
        return ReferencedEnvelope.create(nativeBounds, crs);
    }
}
