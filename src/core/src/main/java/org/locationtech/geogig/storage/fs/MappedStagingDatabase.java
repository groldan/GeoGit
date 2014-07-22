package org.locationtech.geogig.storage.fs;

import java.util.List;

import org.locationtech.geogig.api.Platform;
import org.locationtech.geogig.api.plumbing.merge.Conflict;
import org.locationtech.geogig.repository.RepositoryConnectionException;
import org.locationtech.geogig.storage.AbstractStagingDatabase;
import org.locationtech.geogig.storage.ConfigDatabase;
import org.locationtech.geogig.storage.ObjectDatabase;

import com.google.common.base.Optional;
import com.google.common.base.Suppliers;
import com.google.common.collect.ImmutableList;
import com.google.inject.Inject;

public class MappedStagingDatabase extends AbstractStagingDatabase {

    private ConfigDatabase config;

    @Inject
    public MappedStagingDatabase(ObjectDatabase repositoryDb, Platform platform,
            ConfigDatabase config) {

        super(Suppliers.ofInstance(repositoryDb),//
                Suppliers.ofInstance(new MappedObjectDatabase(platform, config)));
        this.config = config;
    }

    @Override
    public void configure() throws RepositoryConnectionException {
        RepositoryConnectionException.StorageType.STAGING.configure(config, "filedb", "2.0");
    }

    @Override
    public void checkConfig() throws RepositoryConnectionException {
        RepositoryConnectionException.StorageType.STAGING.verify(config, "filedb", "2.0");
    }

    @Override
    public boolean hasConflicts(String namespace) {
        // TODO Auto-generated method stub
        return false;
    }

    @Override
    public Optional<Conflict> getConflict(String namespace, String path) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public List<Conflict> getConflicts(String namespace, String pathFilter) {
        return ImmutableList.of();
    }

    @Override
    public void addConflict(String namespace, Conflict conflict) {
        // TODO Auto-generated method stub

    }

    @Override
    public void removeConflict(String namespace, String path) {
        // TODO Auto-generated method stub

    }

    @Override
    public void removeConflicts(String namespace) {
        // TODO Auto-generated method stub

    }

}
