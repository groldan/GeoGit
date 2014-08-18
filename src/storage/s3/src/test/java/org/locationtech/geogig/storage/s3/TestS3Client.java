package org.locationtech.geogig.storage.s3;

import java.util.HashSet;
import java.util.Set;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.PutObjectResult;

public class TestS3Client extends S3Client {
    private Set<String> addedKeys = new HashSet<>();

    public TestS3Client(AmazonS3 conn, String bucketName) {
        super(conn, bucketName);
    }

    public Set<String> getAddedKeys() {
        return addedKeys;
    }

    @Override
    public PutObjectResult putObject(final String key, final byte[] rawData) {
        PutObjectResult result = super.putObject(key, rawData);
        addedKeys.add(key);
        return result;
    }
}
