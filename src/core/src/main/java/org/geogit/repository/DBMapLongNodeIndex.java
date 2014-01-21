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

import com.google.common.base.Function;
import com.google.common.base.Throwables;
import com.google.common.collect.Iterators;

class DBMapLongNodeIndex implements NodeIndex {
    private static final Random random = new Random();

    private static final Serializer<Node[]> SERIALIZER = new NodeSerializer();

    private static final NodePathStorageOrder NAME_ORDER = new NodePathStorageOrder();

    private DB db;

    private BTreeMap<Long, Node[]> map;

    public DBMapLongNodeIndex(Platform platform) {
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
                .mmapFileEnableIfSupported() //
                .asyncWriteEnable() //
                .cacheSize(10 * 1000)// this influences how fast the index is built at the cost of
                                     // higher memory usage
                // .cacheDisable()//
                // .freeSpaceReclaimQ(5000)//
                .fullChunkAllocationEnable().make();

        map = db.createTreeMap("nodeindex")//
                .valueSerializer(SERIALIZER)//
                .nodeSize(32)//
                .make();

    }

    @Override
    public void close() {
        map.close();
    }

    @Override
    public void add(final Node node) {
        Long key = Long.valueOf(NAME_ORDER.hashCodeLong(node.getName()));
        Node[] nodes = map.get(key);
        if (nodes == null) {
            nodes = new Node[] { node };
        } else {
            Node[] tmp = new Node[nodes.length + 1];
            System.arraycopy(nodes, 0, tmp, 0, nodes.length);
            tmp[nodes.length] = node;
            nodes = tmp;
        }
        map.put(key, nodes);
    }

    @Override
    public Iterator<Node> nodes() {
        Iterator<Node[]> iterator = map.values().iterator();
        Function<Node[], Iterator<Node>> func = new Function<Node[], Iterator<Node>>() {

            @Override
            public Iterator<Node> apply(Node[] samehash) {
                return Iterators.forArray(samehash);
            }
        };
        Iterator<Iterator<Node>> transform = Iterators.transform(iterator, func);
        return Iterators.concat(transform);
    }

    private static class NodeSerializer implements Serializer<Node[]>, Serializable {

        private static final long serialVersionUID = 3899642574574954312L;

        @Override
        public void serialize(DataOutput out, Node[] value) throws IOException {
            out.writeByte(value.length);
            for (Node n : value) {
                FormatCommon.writeNode(n, out);
            }
        }

        @Override
        public Node[] deserialize(DataInput in, final int available) throws IOException {
            if (available == 0) {
                return null;
            }
            int length = in.readByte();
            if (length == 1) {
                return new Node[] { FormatCommon.readNode(in) };
            }
            Node[] nodes = new Node[length];
            for (int i = 0; i < length; i++) {
                nodes[i] = FormatCommon.readNode(in);
            }
            return nodes;
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
