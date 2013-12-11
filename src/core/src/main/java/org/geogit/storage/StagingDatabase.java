/* Copyright (c) 2013 OpenPlans. All rights reserved.
 * This code is licensed under the BSD New License, available at the root
 * application directory.
 */
package org.geogit.storage;

import java.util.Iterator;
import java.util.List;

import javax.annotation.Nullable;

import org.geogit.api.ObjectId;
import org.geogit.api.RevObject;
import org.geogit.api.plumbing.merge.Conflict;

import com.google.common.base.Optional;

/**
 * Provides an interface for GeoGit staging databases.
 * 
 */
public interface StagingDatabase extends ObjectDatabase {

    /**
     * Query method to retrieve a collection of objects from the staging database <b>only</b>, given
     * a collection of object identifiers.
     * <p>
     * This method is similar to {@link #getAllPresent(Iterable, BulkOpListener)} but without the
     * overhead of querying the {@link ObjectDatabase} for objects not found in the staging
     * database.
     * <p>
     * The returned iterator may not preserve the order of the argument list of ids.
     * <p>
     * The {@link BulkOpListener#found(RevObject, Integer) listener.found} method is going to be
     * called for each object found as the returned iterator is traversed.
     * <p>
     * The {@link BulkOpListener#notFound(ObjectId) listener.notFound} method is to be called for
     * each object not found as the iterator is traversed.
     * 
     * @param ids an {@link Iterable} holding the list of ids to remove from the database
     * @param listener a listener that gets notified of {@link BulkOpListener#deleted(ObjectId)
     *        deleted} and {@link BulkOpListener#notFound(ObjectId) not found} items
     * @return an iterator with the objects <b>found</b> on the staging database, in no particular
     *         order
     */
    public Iterator<RevObject> getAllPresentStagingOnly(final Iterable<ObjectId> ids,
            final BulkOpListener listener);

    /**
     * Gets the specified conflict from the database.
     * 
     * @param namespace the namespace of the conflict
     * @param path the conflict to retrieve
     * @return the conflict, or {@link Optional#absent()} if it was not found
     */
    public Optional<Conflict> getConflict(@Nullable String namespace, String path);

    /**
     * Gets all conflicts that match the specified path filter.
     * 
     * @param namespace the namespace of the conflict
     * @param pathFilter the path filter, if this is not defined, all conflicts will be returned
     * @return the list of conflicts
     */
    public List<Conflict> getConflicts(@Nullable String namespace, @Nullable String pathFilter);

    /**
     * Adds a conflict to the database.
     * 
     * @param namespace the namespace of the conflict
     * @param conflict the conflict to add
     */
    public void addConflict(@Nullable String namespace, Conflict conflict);

    /**
     * Removes a conflict from the database.
     * 
     * @param namespace the namespace of the conflict
     * @param path the path of feature whose conflict should be removed
     */
    public void removeConflict(@Nullable String namespace, String path);

    /**
     * Removes all conflicts from the database.
     * 
     * @param namespace the namespace of the conflicts to remove
     */
    public void removeConflicts(@Nullable String namespace);

}