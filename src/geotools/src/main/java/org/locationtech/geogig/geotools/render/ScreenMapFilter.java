package org.locationtech.geogig.geotools.render;

import javax.annotation.Nullable;

import org.geotools.renderer.ScreenMap;
import org.locationtech.geogig.api.Bounded;
import org.locationtech.geogig.api.Bucket;
import org.locationtech.geogig.api.Node;
import org.locationtech.geogig.api.RevObject.TYPE;
import org.opengis.referencing.operation.TransformException;

import com.google.common.base.Predicate;
import com.vividsolutions.jts.geom.Envelope;

class ScreenMapFilter implements Predicate<Bounded> {

    static final class Stats {
        private long skippedTrees, skippedBuckets, skippedFeatures;

        private long acceptedTrees, acceptedBuckets, acceptedFeatures;

        void add(final Bounded b, final boolean skip) {
            Node n = b instanceof Node ? (Node) b : null;
            Bucket bucket = b instanceof Bucket ? (Bucket) b : null;
            if (skip) {
                if (bucket == null) {
                    if (n.getType() == TYPE.FEATURE) {
                        skippedFeatures++;
                    } else {
                        skippedTrees++;
                    }
                } else {
                    skippedBuckets++;
                }
            } else {
                if (bucket == null) {
                    if (n.getType() == TYPE.FEATURE) {
                        acceptedFeatures++;
                    } else {
                        acceptedTrees++;
                    }
                } else {
                    acceptedBuckets++;
                }
            }
        }

        @Override
        public String toString() {
            return String.format(
                    "skipped/accepted: Features(%,d/%,d) Buckets(%,d/%,d) Trees(%,d/%,d)",
                    skippedFeatures, acceptedFeatures, skippedBuckets, acceptedBuckets,
                    skippedTrees, acceptedTrees);
        }
    }

    private ScreenMap screenMap;

    private Envelope envelope = new Envelope();

    private ScreenMapFilter.Stats stats = new Stats();

    public ScreenMapFilter(ScreenMap screenMap) {
        this.screenMap = screenMap;
    }

    public ScreenMapFilter.Stats stats() {
        return stats;
    }

    @Override
    public boolean apply(@Nullable Bounded b) {
        if (b == null) {
            return false;
        }
        envelope.setToNull();
        b.expand(envelope);
        if (envelope.isNull()) {
            return true;
        }
        boolean skip;
        try {
            skip = screenMap.checkAndSet(envelope);
        } catch (TransformException e) {
            e.printStackTrace();
            return true;
        }
        stats.add(b, skip);
        return !skip;
    }
}