/* Copyright (c) 2013 OpenPlans. All rights reserved.
 * This code is licensed under the BSD New License, available at the root
 * application directory.
 */
package org.geogit.osm.internal;

import java.io.File;
import java.util.Collection;
import java.util.Iterator;
import java.util.Map;

import javax.annotation.Nullable;

import org.geotools.data.DataUtilities;
import org.geotools.feature.simple.SimpleFeatureTypeBuilder;
import org.geotools.referencing.CRS;
import org.opengis.feature.simple.SimpleFeatureType;
import org.opengis.referencing.crs.CoordinateReferenceSystem;
import org.openstreetmap.osmosis.core.domain.v0_6.Tag;
import org.openstreetmap.osmosis.xml.common.CompressionMethod;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Throwables;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.vividsolutions.jts.geom.LineString;

public class OSMUtils {

    private static final Logger LOGGER = LoggerFactory.getLogger(OSMUtils.class);

    public static final String OSM_FETCH_BRANCH = "OSM_FETCH";

    public static final String DEFAULT_API_ENDPOINT = "http://overpass-api.de/api/interpreter";

    public static final String FR_API_ENDPOINT = "http://api.openstreetmap.fr/oapi/interpreter/";

    public static final String RU_API_ENDPOINT = "http://overpass.osm.rambler.ru/";

    public static final String NODE_TYPE_NAME = "node";

    public static final String WAY_TYPE_NAME = "way";

    public static final String NAMESPACE = "www.openstreetmap.org";

    private static SimpleFeatureType NodeType;

    public synchronized static SimpleFeatureType nodeType() {
        if (NodeType == null) {
            String typeSpec = "visible:Boolean,version:Integer,timestamp:java.lang.Long,tags:java.util.Map,"
                    + "changeset:java.lang.Long,user:String,location:Point:srid=4326";
            try {
                SimpleFeatureType type = DataUtilities.createType(NAMESPACE,
                        OSMUtils.NODE_TYPE_NAME, typeSpec);
                boolean longitudeFirst = true;
                CoordinateReferenceSystem forceLonLat = CRS.decode("EPSG:4326", longitudeFirst);
                NodeType = DataUtilities.createSubType(type, null, forceLonLat);
            } catch (Exception e) {
                throw Throwables.propagate(e);
            }
        }
        return NodeType;
    }

    private static SimpleFeatureType WayType;

    public synchronized static SimpleFeatureType wayType() {
        if (WayType == null) {
            SimpleFeatureTypeBuilder builder = new SimpleFeatureTypeBuilder();
            builder.add("visible", Boolean.class);
            builder.add("version", Integer.class);
            builder.add("timestamp", Long.class);
            builder.add("tags", java.util.Map.class);
            builder.add("changeset", Long.class);
            builder.add("user", String.class);
            builder.add("nodes", long[].class);
            try {
                builder.add("way", LineString.class, CRS.decode("EPSG:4326", true));
            } catch (Exception e) {
                throw Throwables.propagate(e);
            }
            // String typeSpec =
            // "visible:Boolean,version:Integer,timestamp:java.lang.Long,tags:String,"
            // + "changeset:java.lang.Long,user:String,nodes:String,way:LineString:srid=4326";
            try {
                // SimpleFeatureType type = DataUtilities.createType(NAMESPACE,
                // OSMUtils.WAY_TYPE_NAME, typeSpec);
                // boolean longitudeFirst = true;
                // CoordinateReferenceSystem forceLonLat = CRS.decode("EPSG:4326", longitudeFirst);
                // WayType = DataUtilities.createSubType(type, null, forceLonLat);
                builder.setNamespaceURI(NAMESPACE);
                builder.setName(WAY_TYPE_NAME);
                WayType = builder.buildFeatureType();
            } catch (Exception e) {
                throw Throwables.propagate(e);
            }
        }
        return WayType;
    }

    /**
     * @param collection
     * @return
     */
    @Nullable
    public static String buildTagsString(Collection<Tag> collection) {
        if (collection.isEmpty()) {
            return null;
        }
        StringBuilder sb = new StringBuilder();
        for (Iterator<Tag> it = collection.iterator(); it.hasNext();) {
            Tag e = it.next();
            String key = e.getKey();
            if (key == null || key.isEmpty()) {
                continue;
            }
            String value = e.getValue();
            sb.append(key).append(':').append(value);
            if (it.hasNext()) {
                sb.append('|');
            }
        }
        return sb.toString();
    }

    @Nullable
    public static Map<String, String> buildTagsMap(Iterable<Tag> collection) {
        Map<String, String> tags = Maps.newHashMap();
        String key;
        String value;
        for (Tag e : collection) {
            key = e.getKey();
            if (key == null || key.isEmpty()) {
                continue;
            }
            value = e.getValue();
            tags.put(key, value);
        }
        return tags.isEmpty() ? null : tags;
    }

    public static Collection<Tag> buildTagsCollectionFromString(String tagsString) {
        Collection<Tag> tags = Lists.newArrayList();
        if (tagsString != null) {
            String[] tokens = tagsString.split("\\|");
            for (String token : tokens) {
                int idx = token.lastIndexOf(':');
                if (idx != -1) {
                    Tag tag = new Tag(token.substring(0, idx), token.substring(idx + 1));
                    tags.add(tag);
                } else {
                    LOGGER.info("found tag token '{}' with no value in tagString '{}'", token,
                            tagsString);
                }
            }
        }
        return tags;
    }

    private CompressionMethod resolveCompressionMethod(File file) {
        String fileName = file.getName();
        if (fileName.endsWith(".gz")) {
            return CompressionMethod.GZip;
        } else if (fileName.endsWith(".bz2")) {
            return CompressionMethod.BZip2;
        }
        return CompressionMethod.None;
    }

}
