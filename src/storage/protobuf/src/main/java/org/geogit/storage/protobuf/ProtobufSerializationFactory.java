/* Copyright (c) 2013 OpenPlans. All rights reserved.
 * This code is licensed under the BSD New License, available at the root
 * application directory.
 */
package org.geogit.storage.protobuf;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.Serializable;
import java.util.Map;

import org.geogit.api.ObjectId;
import org.geogit.api.RevCommit;
import org.geogit.api.RevFeature;
import org.geogit.api.RevFeatureType;
import org.geogit.api.RevObject;
import org.geogit.api.RevObject.TYPE;
import org.geogit.api.RevPerson;
import org.geogit.api.RevTree;
import org.geogit.storage.ObjectReader;
import org.geogit.storage.ObjectSerializingFactory;
import org.geogit.storage.ObjectWriter;
import org.geogit.storage.protobuf.RevObjects.Commit;
import org.geogit.storage.protobuf.RevObjects.Person;
import org.geogit.storage.protobuf.RevObjects.Person.Builder;

import com.google.common.collect.ImmutableList;
import com.google.protobuf.ByteString;

public class ProtobufSerializationFactory implements ObjectSerializingFactory {

    private final static ObjectReader<RevCommit> COMMIT_READER = new ObjectReader<RevCommit>() {
        @Override
        public RevCommit read(ObjectId id, InputStream rawData) throws IllegalArgumentException {
            try {
                Commit commit = RevObjects.Commit.parseDelimitedFrom(rawData);
                ObjectId treeId = toId(commit.getTreeId());
                ImmutableList.Builder<ObjectId> parentIds = ImmutableList.builder();
                for (int i = 0; i < commit.getParentIdsCount(); i++) {
                    parentIds.add(toId(commit.getParentIds(i)));
                }
                ImmutableList<ObjectId> parents = parentIds.build();
                RevPerson author = toPerson(commit.getAuthor());
                RevPerson committer = toPerson(commit.getCommitter());
                String message = commit.hasMessage() ? commit.getMessage() : "";
                return new RevCommit(id, treeId, parents, author, committer, message);
            } catch (IOException e) {
                throw new IllegalArgumentException(e);
            }
        }
    };

    private final static ObjectWriter<RevCommit> COMMIT_WRITER = new ObjectWriter<RevCommit>() {
        @Override
        public void write(RevCommit object, OutputStream out) throws IOException {
            Commit commit = toCommit(object);
            commit.writeDelimitedTo(out);
        }
    };

    @SuppressWarnings("unchecked")
    public <T extends RevObject> ObjectWriter<T> createObjectWriter(TYPE type) {
        switch (type) {
        case COMMIT:
            return (ObjectWriter<T>) COMMIT_WRITER;
            // case TREE:
            // return (ObjectWriter<T>) TREE_WRITER;
            // case FEATURE:
            // return (ObjectWriter<T>) FEATURE_WRITER;
            // case FEATURETYPE:
            // return (ObjectWriter<T>) FEATURETYPE_WRITER;
            // case TAG:
            // return (ObjectWriter<T>) TAG_WRITER;
        default:
            throw new UnsupportedOperationException("No writer for " + type);
        }
    }

    private static ObjectId toId(ByteString bytes) {
        return ObjectId.createNoClone(bytes.toByteArray());
    }

    private static RevPerson toPerson(Person p) {
        String name = p.hasName() ? p.getName() : null;
        String email = p.hasEmail() ? p.getEmail() : null;
        long timeStamp = p.getTimeStamp();
        int timeZoneOffset = p.getTimeZoneOffset();
        return new RevPerson(name, email, timeStamp, timeZoneOffset);
    }

    private static Person toPerson(RevPerson p) {
        Person.Builder builder = Person.newBuilder();
        if (p.getName().isPresent()) {
            builder.setName(p.getName().get());
        }
        if (p.getEmail().isPresent()) {
            builder.setEmail(p.getEmail().get());
        }
        builder.setTimeStamp(p.getTimestamp());
        builder.setTimeZoneOffset(p.getTimeZoneOffset());
        return builder.build();
    }

    private static Commit toCommit(RevCommit commit) {
        RevObjects.Commit.Builder builder = Commit.newBuilder();
        builder.setAuthor(toPerson(commit.getAuthor()));
        builder.setCommitter(toPerson(commit.getCommitter()));
        builder.setMessage(commit.getMessage());
        for (int i = 0; i < commit.getParentIds().size(); i++) {
            builder.addParentIds(ByteString.copyFrom(commit.getParentIds().get(i).getRawValue()));
        }
        builder.setTreeId(ByteString.copyFrom(commit.getTreeId().getRawValue()));
        return builder.build();
    }

    @Override
    public ObjectReader<RevCommit> createCommitReader() {
        return COMMIT_READER;
    }

    @Override
    public ObjectReader<RevTree> createRevTreeReader() {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public ObjectReader<RevFeature> createFeatureReader() {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public ObjectReader<RevFeature> createFeatureReader(Map<String, Serializable> hints) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public ObjectReader<RevFeatureType> createFeatureTypeReader() {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public <T> ObjectReader<T> createObjectReader(TYPE type) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public ObjectReader<RevObject> createObjectReader() {
        // TODO Auto-generated method stub
        return null;
    }
    /*
     * 
     * private final static ObjectReader<RevTree> TREE_READER = new TreeReader();
     * 
     * private final static ObjectReader<RevFeature> FEATURE_READER = new FeatureReader();
     * 
     * private final static ObjectReader<RevFeatureType> FEATURETYPE_READER = new
     * FeatureTypeReader();
     * 
     * private final static ObjectReader<RevObject> OBJECT_READER = new
     * org.geogit.storage.datastream.ObjectReader();
     * 
     * private final static ObjectReader<RevTag> TAG_READER = new TagReader();
     * 
     * private final static ObjectWriter<RevCommit> COMMIT_WRITER = new CommitWriter();
     * 
     * private final static ObjectWriter<RevTree> TREE_WRITER = new TreeWriter();
     * 
     * private final static ObjectWriter<RevFeature> FEATURE_WRITER = new FeatureWriter();
     * 
     * private final static ObjectWriter<RevFeatureType> FEATURETYPE_WRITER = new
     * FeatureTypeWriter();
     * 
     * private final static ObjectWriter<RevTag> TAG_WRITER = new TagWriter();
     * 
     * @Override public ObjectReader<RevCommit> createCommitReader() { return COMMIT_READER; }
     * 
     * @Override public ObjectReader<RevTree> createRevTreeReader() { return TREE_READER; }
     * 
     * @Override public ObjectReader<RevFeature> createFeatureReader() { return FEATURE_READER; }
     * 
     * @Override public ObjectReader<RevFeature> createFeatureReader(Map<String, Serializable>
     * hints) { return FEATURE_READER; }
     * 
     * @Override public ObjectReader<RevFeatureType> createFeatureTypeReader() { return
     * FEATURETYPE_READER; }
     * 
     * @Override
     * 
     * @SuppressWarnings("unchecked") public <T extends RevObject> ObjectWriter<T>
     * createObjectWriter(TYPE type) { switch (type) { case COMMIT: return (ObjectWriter<T>)
     * COMMIT_WRITER; case TREE: return (ObjectWriter<T>) TREE_WRITER; case FEATURE: return
     * (ObjectWriter<T>) FEATURE_WRITER; case FEATURETYPE: return (ObjectWriter<T>)
     * FEATURETYPE_WRITER; case TAG: return (ObjectWriter<T>) TAG_WRITER; default: throw new
     * UnsupportedOperationException("No writer for " + type); } }
     * 
     * @Override
     * 
     * @SuppressWarnings("unchecked") public <T> ObjectReader<T> createObjectReader(TYPE type) {
     * switch (type) { case COMMIT: return (ObjectReader<T>) COMMIT_READER; case TREE: return
     * (ObjectReader<T>) TREE_READER; case FEATURE: return (ObjectReader<T>) FEATURE_READER; case
     * FEATURETYPE: return (ObjectReader<T>) FEATURETYPE_READER; case TAG: return (ObjectReader<T>)
     * TAG_READER; default: throw new UnsupportedOperationException("No reader for " + type); } }
     * 
     * @Override public ObjectReader<RevObject> createObjectReader() { return OBJECT_READER; }
     */
}
