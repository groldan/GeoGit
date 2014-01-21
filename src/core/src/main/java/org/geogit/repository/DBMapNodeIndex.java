/* Copyright (c) 2013 OpenPlans. All rights reserved.
 * This code is licensed under the BSD New License, available at the root
 * application directory.
 */
package org.geogit.repository;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.File;
import java.io.IOException;
import java.io.Serializable;
import java.util.Iterator;
import java.util.Random;

import org.geogit.api.Node;
import org.geogit.api.Platform;
import org.geogit.storage.NodePathStorageOrder;
import org.geogit.storage.datastream.FormatCommon;
import org.mapdb.BTreeMap;
import org.mapdb.DB;
import org.mapdb.DBMaker;
import org.mapdb.Serializer;

import com.google.common.base.Throwables;

class DBMapNodeIndex implements NodeIndex {
    private static final Random random = new Random();

    private DB db;

    private BTreeMap<String, Node> map;

    private static final Serializer<Node> SERIALIZER = new NodeSerializer();

    public DBMapNodeIndex(Platform platform) {
        File parent = new File(new File(new File(platform.pwd(), ".geogit"), "tmp"), "nodeindex");
        File file = new File(parent, "nodeindex_" + Math.abs(random.nextInt()));
        parent.mkdirs();
        try {
            file.createNewFile();
        } catch (IOException e) {
            throw Throwables.propagate(e);
        }

        db = DBMaker.newFileDB(file)//
                .transactionDisable()//
                .deleteFilesAfterClose() //
                .asyncWriteEnable()//
                .mmapFileEnableIfSupported()//
                .cacheSize(10 * 1000)//
                // .cacheDisable()//
                .make();

        map = db.createTreeMap("nodeindex")//
                .comparator(new NodePathStorageOrder())//
                .valueSerializer(SERIALIZER)//
                .nodeSize(126)//
                .make();

    }

    @Override
    public void close() {
        map.close();
    }

    @Override
    public void add(Node node) {
        map.put(node.getName(), node);
    }

    @Override
    public Iterator<Node> nodes() {
        return map.values().iterator();
    }

    private static class NodeSerializer implements Serializer<Node>, Serializable {

        private static final long serialVersionUID = 3899642574574954312L;

        @Override
        public void serialize(DataOutput out, Node value) throws IOException {
            FormatCommon.writeNode(value, out);
        }

        @Override
        public Node deserialize(DataInput in, final int available) throws IOException {
            if (available == 0)
                return null;
            return FormatCommon.readNode(in);
        }

        /**
         * @return {@code -1} (no fixed size hint)
         */
        @Override
        public int fixedSize() {
            return -1;
        }
    };

}
