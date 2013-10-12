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

import com.codahale.metrics.annotation.Timed;
import com.google.common.base.Optional;

/**
 * Provides an interface for GeoGit staging databases.
 * 
 */
public interface StagingDatabase extends ObjectDatabase {

    @Timed(name = "StagingDatabase.open", absolute = true)
    @Override
    public void open();

    @Timed(name = "StagingDatabase.close", absolute = true)
    @Override
    public void close();

    @Timed(name = "StagingDatabase.exists", absolute = true)
    @Override
    public boolean exists(final ObjectId id);

    @Timed(name = "StagingDatabase.get", absolute = true)
    @Override
    public RevObject get(ObjectId id) throws IllegalArgumentException;

    @Timed(name = "StagingDatabase.put", absolute = true)
    @Override
    public <T extends RevObject> boolean put(final T object);

    @Timed(name = "StagingDatabase.delete", absolute = true)
    @Override
    public boolean delete(ObjectId objectId);

    @Timed(name = "StagingDatabase.putAll", absolute = true)
    @Override
    public void putAll(Iterator<? extends RevObject> objects);

    @Timed(name = "StagingDatabase.deleteAll", absolute = true)
    @Override
    public long deleteAll(Iterator<ObjectId> ids);

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