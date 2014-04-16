/* Copyright (c) 2013 OpenPlans. All rights reserved.
 * This code is licensed under the BSD New License, available at the root
 * application directory.
 */

package org.geogit.api.plumbing;

import java.util.Iterator;

import org.geogit.api.AbstractGeoGitOp;
import org.geogit.api.plumbing.diff.DiffEntry;
import org.geogit.api.porcelain.DiffOp;

import com.vividsolutions.jts.geom.Envelope;

/**
 * Computes the bounds of the difference between the two trees instead of the actual diffs.
 * 
 */

public class DiffBounds extends AbstractGeoGitOp<Envelope> {

    private String oldVersion;

    private String newVersion;

    private boolean cached;

    public DiffBounds setOldVersion(String oldVersion) {
        this.oldVersion = oldVersion;
        return this;
    }

    public DiffBounds setNewVersion(String newVersion) {
        this.newVersion = newVersion;
        return this;
    }

    public DiffBounds setCompareIndex(boolean cached) {
        this.cached = cached;
        return this;
    }

    @Override
    public Envelope call() {
        DiffOp diff = command(DiffOp.class).setOldVersion(oldVersion).setNewVersion(newVersion)
                .setCompareIndex(cached);
        Iterator<DiffEntry> entries = diff.call();
        Envelope diffBounds = computeDiffBounds(entries);
        return diffBounds;
    }

    /**
     * 
     * @param entries - A list containing each of the DiffEntries
     * @return Envelope - representing the final bounds
     */
    private Envelope computeDiffBounds(Iterator<DiffEntry> entries) {

        Envelope diffBounds = new Envelope();
        diffBounds.setToNull();

        Envelope oldEnvelope = new Envelope();
        Envelope newEnvelope = new Envelope();

        // create a list of envelopes using the entries list
        while (entries.hasNext()) {
            DiffEntry entry = entries.next();
            oldEnvelope.setToNull();
            newEnvelope.setToNull();

            if (entry.getOldObject() != null) {
                entry.getOldObject().expand(oldEnvelope);
            }

            if (entry.getNewObject() != null) {
                entry.getNewObject().expand(newEnvelope);
            }

            if (!oldEnvelope.equals(newEnvelope)) {
                diffBounds.expandToInclude(oldEnvelope);
                diffBounds.expandToInclude(newEnvelope);
            }
        }

        return diffBounds;

    }

}
