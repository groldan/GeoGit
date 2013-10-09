package org.geogit.orientdb;

import org.geogit.api.Platform;
import org.geogit.storage.BlueprintsGraphDatabase;

import com.google.inject.Inject;
import com.tinkerpop.blueprints.impls.orient.OrientGraph;

public class OrientDBGraphDatabase extends BlueprintsGraphDatabase<OrientGraph> {

    @Inject
    public OrientDBGraphDatabase(final Platform platform) {
        super(platform);
    }

    @Override
    protected OrientGraph getGraphDatabase() {
        final String directory = super.dbPath;
        final String url = "local:" + directory;
        OrientGraph orientGraph = new OrientGraph(url);
        return orientGraph;
    }
}
