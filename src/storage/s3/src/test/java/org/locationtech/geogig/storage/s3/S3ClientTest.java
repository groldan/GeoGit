package org.locationtech.geogig.storage.s3;

import static com.google.common.base.Preconditions.checkState;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.locationtech.geogig.api.ObjectId.forString;

import java.io.BufferedReader;
import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.locationtech.geogig.api.ObjectId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.amazonaws.ClientConfiguration;
import com.amazonaws.Protocol;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.Bucket;
import com.amazonaws.services.s3.model.DeleteObjectsRequest;
import com.amazonaws.services.s3.model.DeleteObjectsRequest.KeyVersion;
import com.amazonaws.services.s3.model.DeleteObjectsResult.DeletedObject;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.PutObjectResult;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.S3ObjectInputStream;
import com.google.common.base.Charsets;
import com.google.common.base.Function;
import com.google.common.base.Stopwatch;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.io.ByteStreams;
import com.google.common.io.Files;

public class S3ClientTest {

    private static final Logger LOGGER = LoggerFactory.getLogger(S3ClientTest.class);

    private static String bucketName;

    private static boolean enabled;

    private static AmazonS3 conn;

    private static TestS3Client client;

    @BeforeClass
    public static void beforeClass() throws Exception {
        File propsFile = new File(System.getProperty("user.home"), ".geogig-s3-tests.properties");
        if (!propsFile.exists()) {
            System.err
                    .println(S3Client.class.getSimpleName()
                            + " test disabled. Configure ~/.geogig_s3.properties to run the test with accessKey and secretKey properties");
        }
        String accessKey;
        String secretKey;
        {
            Properties props = new Properties();
            try (BufferedReader reader = Files.newReader(propsFile, Charsets.UTF_8)) {
                props.load(reader);
            }
            accessKey = props.getProperty("accessKey");
            secretKey = props.getProperty("secretKey");
            checkState(!Strings.isNullOrEmpty(accessKey), "accessKey not provided");
            checkState(!Strings.isNullOrEmpty(secretKey), "secretKey not provided");
        }
        AWSCredentials credentials = new BasicAWSCredentials(accessKey, secretKey);

        ClientConfiguration clientConfig = new ClientConfiguration();
        clientConfig.setProtocol(Protocol.HTTP);

        conn = new AmazonS3Client(credentials, clientConfig);
        try {
            conn.listBuckets();
        } catch (RuntimeException e) {
            e.printStackTrace();
            throw e;
        }

        bucketName = "geogig.test." + System.currentTimeMillis();
        Bucket bucket = conn.createBucket(bucketName);
        LOGGER.info("Created bucket {}", bucket);

        client = new TestS3Client(conn, bucketName);
        enabled = true;
    }

    @AfterClass
    public static void afterClass() {
        if (client != null) {
            List<String> addedKeys = new ArrayList<>(client.getAddedKeys());
            if (!addedKeys.isEmpty()) {
                DeleteObjectsRequest req = new DeleteObjectsRequest(bucketName);
                List<KeyVersion> keys = toKeyVersion(addedKeys);
                req.setKeys(keys);
                conn.deleteObjects(req);
            }
            conn.deleteBucket(bucketName);
            LOGGER.info("Deleted bucket {}", bucketName);
        }
    }

    @Test
    public void testPutGet() throws Exception {
        if (!enabled) {
            return;
        }
        ObjectId oid = ObjectId.forString("some hash");
        String key = S3ObjectDatabase.ID_TO_KEY.apply(oid);
        byte[] rawData = new byte[512];
        Arrays.fill(rawData, (byte) 0xfa);

        Stopwatch s = Stopwatch.createStarted();
        PutObjectResult result = client.putObject(key, rawData);
        s.stop();
        LOGGER.info("object put in {}", s);
        assertNotNull(result);

        s.reset().start();
        S3Object object = client.getObject(key);
        s.stop();
        LOGGER.info("object get in {}", s);
        assertNotNull(object);
        assertEquals(rawData.length, (int) object.getObjectMetadata().getContentLength());
        S3ObjectInputStream in = object.getObjectContent();
        byte[] actual = new byte[(int) object.getObjectMetadata().getContentLength()];
        ByteStreams.readFully(in, actual);
        assertTrue(Arrays.equals(rawData, actual));
    }

    @Test
    public void testGetNonExistentObject() {
        if (!enabled) {
            return;
        }
        String key = S3ObjectDatabase.ID_TO_KEY.apply(ObjectId.forString("shall not exist"));
        S3Object object2 = client.getObject(key);
        assertNull(object2);
    }

    @Test
    public void testExists() {
        if (!enabled) {
            return;
        }
        String existsKey = put(forString("exists"), new byte[128]);
        String doesntExistKey = S3ObjectDatabase.ID_TO_KEY.apply(forString("doesnt exist"));

        ObjectMetadata metadata = client.getMetadata(existsKey);
        assertNotNull(metadata);
        assertEquals(128, (int) metadata.getContentLength());

        metadata = client.getMetadata(doesntExistKey);
        assertNull(metadata);
    }

    @Test
    public void testDeleteObjects() {
        String k1 = put(forString("k1"), new byte[64]);
        String k2 = put(forString("k2"), new byte[64]);
        String k3 = put(forString("k3"), new byte[64]);
        String k4 = put(forString("k4"), new byte[64]);

        List<DeletedObject> deleted;
        deleted = client.deleteObjects(toKeyVersion(ImmutableList.of(k2)));
        assertEquals(1, deleted.size());

        deleted = client.deleteObjects(toKeyVersion(ImmutableList.of("keynotfound")));
        assertEquals(deleted.toString(), 0, deleted.size());

    }

    private static String put(ObjectId id, byte[] data) {
        String key = S3ObjectDatabase.ID_TO_KEY.apply(id);
        assertNotNull(client.putObject(key, data));
        return key;
    }

    private static List<KeyVersion> toKeyVersion(List<String> addedKeys) {
        return Lists.transform(addedKeys, new Function<String, KeyVersion>() {
            @Override
            public KeyVersion apply(String key) {
                return new KeyVersion(key);
            }
        });
    }
}
