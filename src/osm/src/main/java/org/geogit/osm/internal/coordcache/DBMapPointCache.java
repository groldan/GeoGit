/* Copyright (c) 2013 OpenPlans. All rights reserved.
 * This code is licensed under the BSD New License, available at the root
 * application directory.
 */
package org.geogit.osm.internal.coordcache;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.File;
import java.io.IOException;
import java.io.Serializable;
import java.util.List;
import java.util.Map;
import java.util.Random;

import org.geogit.api.Platform;
import org.geogit.osm.internal.OSMCoordinateSequence;
import org.geogit.osm.internal.OSMCoordinateSequenceFactory;
import org.mapdb.DB;
import org.mapdb.DBMaker;
import org.mapdb.Serializer;

import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import com.vividsolutions.jts.geom.CoordinateSequence;

/**
 * A {@link PointCache} that works over a temporary large hashmap provided by <a
 * href="http://www.mapdb.org/">mapdb</a>, which uses off-heap storage through memory mapped files
 * inside the repository's {@code .geogit/tmp/pointcache} directory, and provides for collections
 * larger than {@link Integer#MAX_VALUE Integer.MAX_VALUE}.
 * 
 */
public class DBMapPointCache implements PointCache {

    private static final OSMCoordinateSequenceFactory CSFAC = OSMCoordinateSequenceFactory
            .instance();

    private static final CoordinateSerializer VALUE_SERIALIZER = new CoordinateSerializer();

    private static final Random random = new Random();

    private DB db;

    private Map<Long, int[]> map;

    public DBMapPointCache(Platform platform) {
        File parent = new File(new File(new File(platform.pwd(), ".geogit"), "tmp"), "pointcache");
        File file = new File(parent, "pointCache_" + Math.abs(random.nextInt()));
        parent.mkdirs();
        try {
            file.createNewFile();
        } catch (IOException e) {
            throw Throwables.propagate(e);
        }
        db = DBMaker.newFileDB(file)//
                .transactionDisable()//
                .asyncWriteEnable() //
                .mmapFileEnableIfSupported()//
                // .strictDBGet()//
                .deleteFilesAfterClose()//
                .cacheSize(10 * 1000)//
                .make();

        map = db.createTreeMap("cache").valueSerializer(VALUE_SERIALIZER).nodeSize(126)
                .makeLongMap();
    }

    @Override
    public void put(Long nodeId, OSMCoordinateSequence coord) {
        Preconditions.checkNotNull(nodeId, "id is null");
        Preconditions.checkNotNull(coord, "coord is null");
        Preconditions.checkArgument(1 == coord.size(), "coord list size is not 1");
        int[] c = coord.ordinates();
        map.put(nodeId, c);
    }

    @Override
    public void dispose() {
        if (map == null) {
            return;
        }
        try {
            db.close();
        } finally {
            map = null;
            db = null;
        }
    }

    @Override
    public CoordinateSequence get(List<Long> ids) {
        Preconditions.checkNotNull(ids, "ids is null");
        final int size = ids.size();
        OSMCoordinateSequence coords = CSFAC.create(size);
        int[] osmCoord;
        Long nodeId;
        for (int index = 0; index < size; index++) {
            nodeId = ids.get(index);
            osmCoord = map.get(nodeId);
            Preconditions
                    .checkArgument(osmCoord != null, "No coordinate found for node %s", nodeId);
            coords.setOrdinate(index, 0, osmCoord[0]);
            coords.setOrdinate(index, 1, osmCoord[1]);
        }
        return coords;
    }

    private static class CoordinateSerializer implements Serializer<int[]>, Serializable {

        private static final long serialVersionUID = 2190666842027015431L;

        @Override
        public void serialize(DataOutput out, int[] c) throws IOException {
            out.writeInt(c[0]);
            out.writeInt(c[1]);
        }

        @Override
        public int[] deserialize(DataInput in, final int available) throws IOException {
            return new int[] { in.readInt(), in.readInt() };
        }

        @Override
        public int fixedSize() {
            return 8;
        }
    };
}
