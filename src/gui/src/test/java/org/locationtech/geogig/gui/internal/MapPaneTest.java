package org.locationtech.geogig.gui.internal;

import org.geogit.gui.internal.MapPane;
import org.junit.Ignore;
import org.junit.Test;
import org.locationtech.geogig.api.GeoGIG;
import org.locationtech.geogig.test.integration.RepositoryTestCase;

public class MapPaneTest extends RepositoryTestCase {

    @Override
    protected void setUpInternal() throws Exception {
        insertAndAdd(points1, points2, points3);
        // commit("points");
        insertAndAdd(lines1, lines2, lines3);
        // commit("lines");
    }

    @Test
    @Ignore
    public void show() throws Exception {
        GeoGIG geogig = getGeogig();
        MapPane mapPane = new MapPane(geogig);
        mapPane.show();
        Thread.sleep(2000);
    }
}
