/* Copyright (c) 2013 OpenPlans. All rights reserved.
 * This code is licensed under the BSD New License, available at the root
 * application directory.
 */
package org.geogit.storage.bdbje;

import java.io.File;
import java.io.IOException;
import java.net.URISyntaxException;
import java.net.URL;
import java.util.List;

import javax.annotation.Nullable;

import org.geogit.api.Platform;
import org.geogit.api.RevTree;
import org.geogit.api.plumbing.ResolveGeogitDir;
import org.geogit.api.plumbing.merge.Conflict;
import org.geogit.repository.RepositoryConnectionException;
import org.geogit.storage.ConfigDatabase;
import org.geogit.storage.ForwardingStagingDatabase;
import org.geogit.storage.ObjectDatabase;
import org.geogit.storage.ObjectSerializingFactory;

import com.google.common.base.Charsets;
import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import com.google.common.base.Supplier;
import com.google.common.base.Suppliers;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.io.Files;
import com.google.common.io.LineProcessor;
import com.google.inject.Inject;
import com.sleepycat.je.Environment;

/**
 * The Index (or Staging Area) object database.
 * <p>
 * This is a composite object database holding a reference to the actual repository object database
 * and a separate object database for the staging area itself.
 * <p>
 * Object look ups are first performed against the staging area database. If the object is not
 * found, then the look up is deferred to the actual repository database.
 * <p>
 * Object writes are always performed against the staging area object database.
 * <p>
 * The staging area database holds references to two root {@link RevTree trees}, one for the staged
 * objects and another one for the unstaged objects. When objects are added/changed/deleted to/from
 * the index, those modifications are written to the unstaged root tree. When objects are staged to
 * be committed, the unstaged objects are moved to the staged root tree.
 * <p>
 * A diff operation between the repository root tree and the index unstaged root tree results in the
 * list of unstaged objects.
 * <p>
 * A diff operation between the repository root tree and the index staged root tree results in the
 * list of staged objects.
 * 
 */
public class JEStagingDatabase extends ForwardingStagingDatabase {

    private Platform platform;

    private ConfigDatabase configDB;

    private Environment tempDatabasesEnvironment;

    /**
     * @param referenceDatabase the repository reference database, used to get the head re
     * @param repoDb
     * @param stagingDb
     */
    @Inject
    public JEStagingDatabase(final ObjectSerializingFactory sfac,
            final ObjectDatabase repositoryDb, final EnvironmentBuilder envBuilder,
            final Platform platform, final ConfigDatabase configDB) {

        super(Suppliers.ofInstance(repositoryDb), stagingDbSupplier(sfac, envBuilder, configDB));

        this.platform = platform;
        this.configDB = configDB;
    }

    private static Supplier<JEObjectDatabase> stagingDbSupplier(
            final ObjectSerializingFactory sfac, final EnvironmentBuilder envProvider,
            final ConfigDatabase configDb) {

        return Suppliers.memoize(new Supplier<JEObjectDatabase>() {

            @Override
            public JEObjectDatabase get() {
                envProvider.setRelativePath("index");
                envProvider.setIsStagingDatabase(true);
                Environment env = envProvider.get();
                JEObjectDatabase db = new JEObjectDatabase(sfac, env, configDb);
                return db;
            }
        });
    }

    @Override
    public void close() {
        try {
            if (tempDatabasesEnvironment != null) {
                tempDatabasesEnvironment.close();
            }
        } finally {
            super.close();
        }
    }

    // TODO:
    // *****************************************************************************************
    // The following methods are a temporary implementation of conflict storage that relies on a
    // conflict file in the index folder
    // *****************************************************************************************

    /**
     * Gets all conflicts that match the specified path filter.
     * 
     * @param namespace the namespace of the conflict
     * @param pathFilter the path filter, if this is not defined, all conflicts will be returned
     * @return the list of conflicts
     */
    @Override
    public List<Conflict> getConflicts(@Nullable String namespace, final String pathFilter) {
        Optional<File> conflictsFile = findOrCreateConflictsFile(namespace);
        if (!conflictsFile.isPresent()) {
            return ImmutableList.of();
        }
        File file = conflictsFile.get();
        List<Conflict> conflicts = Lists.newArrayList();
        try {
            synchronized (file.getCanonicalPath().intern()) {
                conflicts = Files.readLines(file, Charsets.UTF_8,
                        new LineProcessor<List<Conflict>>() {
                            List<Conflict> conflicts = Lists.newArrayList();

                            @Override
                            public List<Conflict> getResult() {
                                return conflicts;
                            }

                            @Override
                            public boolean processLine(String s) throws IOException {
                                Conflict c = Conflict.valueOf(s);
                                if (pathFilter == null) {
                                    conflicts.add(c);
                                } else if (c.getPath().startsWith(pathFilter)) {
                                    conflicts.add(c);
                                }
                                return true;
                            }
                        });
            }
        } catch (IOException e) {
            throw Throwables.propagate(e);
        }
        return conflicts;
    }

    /**
     * Adds a conflict to the database.
     * 
     * @param namespace the namespace of the conflict
     * @param conflict the conflict to add
     */
    @Override
    public void addConflict(@Nullable String namespace, Conflict conflict) {
        Optional<File> file = findOrCreateConflictsFile(namespace);
        Preconditions.checkState(file.isPresent());
        try {
            Files.append(conflict.toString() + "\n", file.get(), Charsets.UTF_8);
        } catch (IOException e) {
            throw Throwables.propagate(e);
        }
    }

    /**
     * Removes a conflict from the database.
     * 
     * @param namespace the namespace of the conflict
     * @param path the path of feature whose conflict should be removed
     */
    @Override
    public void removeConflict(@Nullable String namespace, String path) {
        List<Conflict> conflicts = getConflicts(namespace, null);
        Optional<File> file = findOrCreateConflictsFile(namespace);
        Preconditions.checkState(file.isPresent());

        StringBuilder sb = new StringBuilder();
        try {
            for (Conflict conflict : conflicts) {
                if (!path.equals(conflict.getPath())) {
                    sb.append(conflict.toString() + "\n");
                }
            }
            String s = sb.toString();
            if (!s.isEmpty()) {
                Files.write(s, file.get(), Charsets.UTF_8);
            } else {
                file.get().delete();
            }
        } catch (IOException e) {
            throw Throwables.propagate(e);
        }
    }

    /**
     * Gets the specified conflict from the database.
     * 
     * @param namespace the namespace of the conflict
     * @param path the conflict to retrieve
     * @return the conflict, or {@link Optional#absent()} if it was not found
     */
    @Override
    public Optional<Conflict> getConflict(@Nullable String namespace, final String path) {
        Optional<File> file = findOrCreateConflictsFile(namespace);
        if (!file.isPresent()) {
            return Optional.absent();
        }
        Conflict conflict = null;
        try {
            File conflictsFile = file.get();
            synchronized (conflictsFile.getCanonicalPath().intern()) {
                conflict = Files.readLines(conflictsFile, Charsets.UTF_8,
                        new LineProcessor<Conflict>() {
                            Conflict conflict = null;

                            @Override
                            public Conflict getResult() {
                                return conflict;
                            }

                            @Override
                            public boolean processLine(String s) throws IOException {
                                Conflict c = Conflict.valueOf(s);
                                if (c.getPath().equals(path)) {
                                    conflict = c;
                                    return false;
                                } else {
                                    return true;
                                }
                            }
                        });
            }
        } catch (IOException e) {
            throw Throwables.propagate(e);
        }
        return Optional.fromNullable(conflict);
    }

    private Optional<File> findOrCreateConflictsFile(@Nullable String namespace) {
        if (namespace == null) {
            namespace = "conflicts";
        }
        Optional<File> conflicts = Optional.absent();
        if (isOpen()) {
            Optional<URL> repoPath = new ResolveGeogitDir(platform).call();
            File file = null;
            try {
                file = new File(repoPath.get().toURI());
            } catch (URISyntaxException e1) {
                Throwables.propagate(e1);
            }
            file = new File(file, namespace);
            if (!file.exists()) {
                try {
                    file.getParentFile().mkdirs();
                    file.createNewFile();
                } catch (IOException e) {
                    throw Throwables.propagate(e);
                }
            }
            conflicts = Optional.of(file);
        }
        return conflicts;
    }

    /**
     * Removes all conflicts from the database.
     * 
     * @param namespace the namespace of the conflicts to remove
     */
    @Override
    public void removeConflicts(@Nullable String namespace) {
        Optional<File> file = findOrCreateConflictsFile(namespace);
        if (file.isPresent()) {
            file.get().delete();
        }
    }

    @Override
    public void configure() throws RepositoryConnectionException {
        RepositoryConnectionException.StorageType.STAGING.configure(configDB, "bdbje", "0.1");
    }

    @Override
    public void checkConfig() throws RepositoryConnectionException {
        RepositoryConnectionException.StorageType.STAGING.verify(configDB, "bdbje", "0.1");
    }
}
