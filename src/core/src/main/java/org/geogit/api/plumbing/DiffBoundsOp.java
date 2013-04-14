/* Copyright (c) 2013 OpenPlans. All rights reserved.
 * This code is licensed under the BSD New License, available at the root
 * application directory.
 */

package org.geogit.api.plumbing;

import java.util.List;

import javax.annotation.Nullable;

import org.geogit.api.AbstractGeoGitOp;
import org.geogit.api.ObjectId;
import org.geogit.api.Ref;
import org.geogit.api.RevTree;
import org.geogit.api.plumbing.diff.DiffBoundsCalculator;
import org.geogit.api.plumbing.diff.DiffTreeWalk;
import org.geogit.storage.StagingDatabase;
import org.geotools.geometry.jts.ReferencedEnvelope;

import com.google.common.base.Optional;
import com.google.common.collect.Lists;
import com.google.inject.Inject;

/**
 * Computes the spatial extent affected by the differences between two trees.
 * <p>
 * This is a faster alternative to compute the affected bounds of diffs between two trees than
 * walking a {@link DiffTreeWalk} iterator.
 * 
 */
public class DiffBoundsOp extends AbstractGeoGitOp<ReferencedEnvelope> {

    private StagingDatabase index;

    private final List<String> pathFilters = Lists.newLinkedList();

    private String oldRefSpec;

    private String newRefSpec;

    @Inject
    public DiffBoundsOp(StagingDatabase index) {
        this.index = index;
    }

    /**
     * @param refSpec tree-ish representing the left side of the diff, defaults to {@link Ref#HEAD
     *        'HEAD'}
     */
    public DiffBoundsOp setOldVersion(@Nullable String refSpec) {
        this.oldRefSpec = refSpec;
        return this;
    }

    /**
     * @param refSpec tree-ish representing the right side of the diff, defaults to {@link
     *        Ref#WORK_HEAD 'WORK_HEAD'}
     */
    public DiffBoundsOp setNewVersion(@Nullable String refSpec) {
        this.newRefSpec = refSpec;
        return this;
    }

    /**
     * @param path the path filter to use during the diff operation
     * @return {@code this}
     */
    public DiffBoundsOp addFilter(@Nullable String path) {
        if (path != null) {
            pathFilters.add(path);
        }
        return this;
    }

    /**
     * @param paths list of paths to filter by, if {@code null} or empty, then no filtering is done,
     *        otherwise the list must not contain null elements.
     */
    public DiffBoundsOp setFilter(@Nullable List<String> paths) {
        pathFilters.clear();
        if (paths != null) {
            pathFilters.addAll(paths);
        }
        return this;
    }

    @Override
    public ReferencedEnvelope call() {
        final RevTree oldTree = getTree(oldRefSpec, Ref.HEAD);
        final RevTree newTree = getTree(newRefSpec, Ref.WORK_HEAD);

        DiffBoundsCalculator calculator = new DiffBoundsCalculator(index, oldTree, newTree);
        calculator.setPathFilters(this.pathFilters);
        ReferencedEnvelope diffBounds = calculator.get();
        return diffBounds;
    }

    /**
     * @return the tree referenced by the old ref, or the head of the index.
     */
    private RevTree getTree(@Nullable String refSpec, final String defaultRefSpec) {

        final String spec = refSpec == null ? defaultRefSpec : refSpec;
        Optional<ObjectId> resolved = command(ResolveTreeish.class).setTreeish(spec).call();
        if (!resolved.isPresent()) {
            return RevTree.EMPTY;
        }
        ObjectId headTreeId = resolved.get();
        final RevTree headTree;
        if (headTreeId.isNull()) {
            headTree = RevTree.EMPTY;
        } else {
            headTree = command(RevObjectParse.class).setObjectId(headTreeId).call(RevTree.class)
                    .get();
        }

        return headTree;
    }

}
