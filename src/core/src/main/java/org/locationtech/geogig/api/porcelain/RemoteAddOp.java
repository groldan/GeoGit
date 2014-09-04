/* Copyright (c) 2012-2014 Boundless and others.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Eclipse Distribution License v1.0
 * which accompanies this distribution, and is available at
 * https://www.eclipse.org/org/documents/edl-v10.html
 *
 * Contributors:
 * Johnathan Garrett (LMN Solutions) - initial implementation
 */
package org.locationtech.geogig.api.porcelain;

import java.util.List;

import org.locationtech.geogig.api.AbstractGeoGigOp;
import org.locationtech.geogig.api.Ref;
import org.locationtech.geogig.api.Remote;
import org.locationtech.geogig.api.porcelain.RemoteException.StatusCode;
import org.locationtech.geogig.storage.ConfigDatabase;

/**
 * Adds a remote to the local config database.
 * 
 * @see ConfigDatabase
 * @see Remote
 */
public class RemoteAddOp extends AbstractGeoGigOp<Remote> {

    private String name;

    private String url;

    private String branch;

    private String username;

    private String password;

    private boolean mapped = false;

    /**
     * Executes the remote-add operation.
     * 
     * @return the {@link Remote} that was added.
     */
    @Override
    protected Remote _call() {
        if (name == null || name.isEmpty()) {
            throw new RemoteException(StatusCode.MISSING_NAME);
        }
        if (url == null || url.isEmpty()) {
            throw new RemoteException(StatusCode.MISSING_URL);
        }
        if (branch == null || branch.isEmpty()) {
            branch = "*";
        }

        ConfigDatabase config = configDatabase();
        List<String> allRemotes = config.getAllSubsections("remote");
        if (allRemotes.contains(name)) {
            throw new RemoteException(StatusCode.REMOTE_ALREADY_EXISTS);
        }

        String configSection = "remote." + name;
        String fetch = "+" + Ref.HEADS_PREFIX + branch + ":" + Ref.REMOTES_PREFIX + name + "/"
                + branch;

        config.put(configSection + ".url", url);
        config.put(configSection + ".fetch", fetch);
        if (mapped) {
            config.put(configSection + ".mapped", "true");
            config.put(configSection + ".mappedBranch", branch);
        }
        if (username != null) {
            config.put(configSection + ".username", username);
        }
        if (password != null) {
            password = Remote.encryptPassword(password);
            config.put(configSection + ".password", password);
        }

        return new Remote(name, url, url, fetch, mapped, branch, username, password);
    }

    /**
     * @param name the name of the remote
     * @return {@code this}
     */
    public RemoteAddOp setName(String name) {
        this.name = name;
        return this;
    }

    /**
     * @param url the URL of the remote
     * @return {@code this}
     */
    public RemoteAddOp setURL(String url) {
        this.url = url;
        return this;
    }

    /**
     * @param branch a specific branch to track
     * @return {@code this}
     */
    public RemoteAddOp setBranch(String branch) {
        this.branch = branch;
        return this;
    }

    /**
     * @param username user name for the repository
     * @return {@code this}
     */
    public RemoteAddOp setUserName(String username) {
        this.username = username;
        return this;
    }

    /**
     * @param password password for the repository
     * @return {@code this}
     */
    public RemoteAddOp setPassword(String password) {
        this.password = password;
        return this;
    }

    /**
     * @param mapped whether or not this is a mapped remote
     * @return {@code this}
     */
    public RemoteAddOp setMapped(boolean mapped) {
        this.mapped = mapped;
        return this;
    }

}
