package org.locationtech.geogig.storage.s3;

import java.io.ByteArrayInputStream;
import java.util.List;

import javax.annotation.Nullable;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.iterable.S3Objects;
import com.amazonaws.services.s3.model.AmazonS3Exception;
import com.amazonaws.services.s3.model.DeleteObjectsRequest;
import com.amazonaws.services.s3.model.DeleteObjectsRequest.KeyVersion;
import com.amazonaws.services.s3.model.DeleteObjectsResult;
import com.amazonaws.services.s3.model.DeleteObjectsResult.DeletedObject;
import com.amazonaws.services.s3.model.GetObjectMetadataRequest;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.amazonaws.services.s3.model.PutObjectResult;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.S3ObjectSummary;

class S3Client {

    private String bucketName;

    private AmazonS3 conn;

    public S3Client(final AmazonS3 conn, final String bucketName) {
        this.conn = conn;
        this.bucketName = bucketName;
    }

    public PutObjectResult putObject(final String key, final byte[] rawData) {
        ObjectMetadata metadata = new ObjectMetadata();
        metadata.setContentLength(rawData.length);
        metadata.setContentEncoding("lzw");
        metadata.setContentType("application/octet-stream");
        PutObjectRequest req = new PutObjectRequest(bucketName, key, new ByteArrayInputStream(
                rawData), metadata);
        PutObjectResult result = conn.putObject(req);
        return result;
    }

    /**
     * @return the object for the given key, or {@code null} if no such key exists in the bucket
     */
    @Nullable
    public S3Object getObject(final String key) {
        GetObjectRequest req = new GetObjectRequest(bucketName, key);
        S3Object s3Object;
        try {
            s3Object = conn.getObject(req);
        } catch (AmazonS3Exception e) {
            if (404 == e.getStatusCode()) {
                return null;
            }
            throw e;
        }
        return s3Object;
    }

    @Nullable
    public ObjectMetadata getMetadata(final String key) {
        GetObjectMetadataRequest req = new GetObjectMetadataRequest(bucketName, key);
        ObjectMetadata metadata;
        try {
            metadata = conn.getObjectMetadata(req);
        } catch (AmazonS3Exception e) {
            if (404 == e.getStatusCode()) {
                return null;
            }
            throw e;
        }
        return metadata;
    }

    public List<DeletedObject> deleteObjects(final List<KeyVersion> keys) {
        DeleteObjectsRequest req = new DeleteObjectsRequest(bucketName);
        req.setKeys(keys);
        DeleteObjectsResult deleteResults = conn.deleteObjects(req);
        List<DeletedObject> successfullyDeletedObjects = deleteResults.getDeletedObjects();
        return successfullyDeletedObjects;
    }

    public Iterable<S3ObjectSummary> listWithPrefix(final String prefix) {
        S3Objects matches = S3Objects.withPrefix(conn, bucketName, prefix);
        return matches;
    }

}
