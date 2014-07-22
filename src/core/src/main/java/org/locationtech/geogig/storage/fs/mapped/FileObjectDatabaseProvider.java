package org.locationtech.geogig.storage.fs.mapped;

import java.io.File;

import org.locationtech.geogig.api.Platform;
import org.locationtech.geogig.api.plumbing.ResolveGeogigDir;
import org.locationtech.geogig.storage.ConfigDatabase;

import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import com.google.inject.Provider;

public class FileObjectDatabaseProvider implements Provider<MappedObjectDatabase> {

    private Provider<Platform> platformProvider;

    private Provider<ConfigDatabase> config;

    public FileObjectDatabaseProvider(Provider<Platform> platformProvider,
            Provider<ConfigDatabase> config) {
        this.platformProvider = platformProvider;
        this.config = config;
    }

    @Override
    public MappedObjectDatabase get() {
        Platform platform = platformProvider.get();
        Optional<File> geogitDir = new ResolveGeogigDir(platform).getFile();
        Preconditions.checkState(geogitDir.isPresent(), "Not inside a geogit dir");
        File environment = new File(geogitDir.get(), "objects");
        Preconditions.checkState(
                (environment.exists() && environment.isDirectory()) || environment.mkdir(),
                "Can't access or create directory '%s'", environment.getAbsolutePath());

        ConfigDatabase configDatabase = config.get();
        return new MappedObjectDatabase(environment, configDatabase);
    }

}
