/* Copyright (c) 2013 OpenPlans. All rights reserved.
 * This code is licensed under the BSD New License, available at the root
 * application directory.
 */
package org.geogit.api.porcelain;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.net.MalformedURLException;
import java.net.URISyntaxException;
import java.net.URL;

import org.geogit.api.AbstractGeoGitOp;
import org.geogit.api.ObjectId;
import org.geogit.api.Platform;
import org.geogit.api.Ref;
import org.geogit.api.plumbing.RefParse;
import org.geogit.api.plumbing.ResolveGeogitDir;
import org.geogit.api.plumbing.UpdateRef;
import org.geogit.api.plumbing.UpdateSymRef;
import org.geogit.di.CanRunDuringConflict;
import org.geogit.repository.Repository;
import org.geogit.repository.RepositoryConnectionException;

import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import com.google.common.io.Resources;
import com.google.inject.Inject;
import com.google.inject.Injector;

/**
 * Creates or "initializes" a repository in the {@link Platform#pwd() working directory}.
 * <p>
 * This command tries to find an existing {@code .geogit} repository directory in the current
 * directory's hierarchy. It is safe to call it from inside a directory that's a child of a
 * repository.
 * <p>
 * If no repository directory is found, then a new one is created on the current directory.
 * 
 * @see ResolveGeogitDir
 * @see RefParse
 * @see UpdateRef
 * @see UpdateSymRef
 */
@CanRunDuringConflict
public class InitOp extends AbstractGeoGitOp<Repository> {

    private Platform platform;

    private Injector injector;

    /**
     * Constructs a new {@code InitOp} with the specified parameters.
     * 
     * @param platform where to get the current directory from
     * @param injector where to get the repository from (with auto-wired dependencies) once ensured
     *        the {@code .geogit} repository directory is found or created.
     */
    @Inject
    public InitOp(Platform platform, Injector injector) {
        checkNotNull(platform);
        checkNotNull(injector);
        this.platform = platform;
        this.injector = injector;
    }

    /**
     * Executes the Init operation.
     * 
     * @return the repository _if_ it was newly created, {@code null} if an existing repository was
     *         reinitialized (NOTE so far reinitialized is a loose term, we're not reinitializing
     *         anything, like copying template config files to the repo directory or so)
     * @throws IllegalStateException if a repository cannot be created on the current directory or
     *         re-initialized in the current dir or one if its parents as determined by
     *         {@link ResolveGeogitDir}
     */
    @Override
    public Repository call() {
        final File workingDirectory = platform.pwd();
        checkState(workingDirectory != null, "working directory is null");
        final Optional<URL> repoUrl = new ResolveGeogitDir(platform).call();

        boolean repoExisted = false;
        File envHome;
        if (!repoUrl.isPresent()) {
            envHome = new File(workingDirectory, ".geogit");
            envHome.mkdirs();
            if (!envHome.exists()) {
                throw new RuntimeException("Unable to create geogit environment at '"
                        + envHome.getAbsolutePath() + "'");
            }
        } else {
            // we're at either the repo working dir or a subdirectory of it
            try {
                envHome = new File(repoUrl.get().toURI());
            } catch (URISyntaxException e) {
                throw Throwables.propagate(e);
            }
            repoExisted = true;
        }

        try {
            Preconditions.checkState(envHome.toURI().toURL()
                    .equals(new ResolveGeogitDir(platform).call().get()));
        } catch (MalformedURLException e) {
            Throwables.propagate(e);
        }

        Repository repository;
        try {
            repository = injector.getInstance(Repository.class);
            if (!repoExisted) {
                try {
                    repository.configure();
                } catch (RepositoryConnectionException e) {
                    throw new IllegalStateException(
                            "Unable to initialize repository for the first time: " + e.getMessage(),
                            e);
                }
            }
            try {
                repository.open();
            } catch (RepositoryConnectionException e) {
                throw new IllegalStateException("Error opening repository databases: "
                        + e.getMessage(), e);
            }
            createSampleHooks(envHome);
        } catch (RuntimeException e) {
            Throwables.propagateIfInstanceOf(e, IllegalStateException.class);
            throw new IllegalStateException("Can't access repository at '"
                    + envHome.getAbsolutePath() + "'", e);
        }

        if (!repoExisted) {
            try {
                createDefaultRefs();
            } catch (IllegalStateException e) {
                Throwables.propagate(e);
            }
        }

        return repoExisted ? null : repository;
    }

    private void createSampleHooks(File envHome) {

        File hooks = new File(envHome, "hooks");
        hooks.mkdirs();
        if (!hooks.exists()) {
            throw new RuntimeException();
        }
        try {
            copyHookFile(hooks.getAbsolutePath(), "pre_commit.js.sample");
            // TODO: add other example hooks
        } catch (IOException e) {
            throw new RuntimeException();
        }

    }

    private void copyHookFile(String folder, String file) throws IOException {

        URL url = Resources.getResource("org/geogit/api/hooks/" + file);
        OutputStream os = new FileOutputStream(new File(folder, file).getAbsolutePath());
        Resources.copy(url, os);
        os.close();

    }

    private void createDefaultRefs() {
        Optional<Ref> master = command(RefParse.class).setName(Ref.MASTER).call();
        Preconditions.checkState(!master.isPresent(), Ref.MASTER + " was already initialized.");
        command(UpdateRef.class).setName(Ref.MASTER).setNewValue(ObjectId.NULL)
                .setReason("Repository initialization").call();

        Optional<Ref> head = command(RefParse.class).setName(Ref.HEAD).call();
        Preconditions.checkState(!head.isPresent(), Ref.HEAD + " was already initialized.");
        command(UpdateSymRef.class).setName(Ref.HEAD).setNewValue(Ref.MASTER)
                .setReason("Repository initialization").call();

        Optional<Ref> workhead = command(RefParse.class).setName(Ref.WORK_HEAD).call();
        Preconditions
                .checkState(!workhead.isPresent(), Ref.WORK_HEAD + " was already initialized.");
        command(UpdateRef.class).setName(Ref.WORK_HEAD).setNewValue(ObjectId.NULL)
                .setReason("Repository initialization").call();

        Optional<Ref> stagehead = command(RefParse.class).setName(Ref.STAGE_HEAD).call();
        Preconditions.checkState(!stagehead.isPresent(), Ref.STAGE_HEAD
                + " was already initialized.");
        command(UpdateRef.class).setName(Ref.STAGE_HEAD).setNewValue(ObjectId.NULL)
                .setReason("Repository initialization").call();

    }
}
