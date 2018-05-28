import com.amazonaws.ClientConfiguration;
import com.amazonaws.Protocol;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.glacier.model.EncryptionType;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.AmazonS3EncryptionClientBuilder;
import com.amazonaws.services.s3.model.*;
import com.amazonaws.util.Base64;
import org.apache.commons.lang3.StringUtils;

import javax.crypto.SecretKey;
import javax.crypto.spec.SecretKeySpec;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;


/**
 * Created by LOCNGUYEN on 5/28/2018.
 */
public class AmazonS3FileTransfer extends FileTransfer{
    private String bucketName;
    private Regions region;
    private AWSCredentialsProvider credentialProvider;
    private AmazonS3 s3Client;

    private EncryptionType encryptionType;
    private String clientSideBase64AesEncryptionKey;
    private String sseAlgorithm;
    private boolean transferAcceleration = false;
    private Protocol protocol = Protocol.HTTPS;
    private int connectionTimeout = ClientConfiguration.DEFAULT_CONNECTION_TIMEOUT;
    private int socketTimeout = ClientConfiguration.DEFAULT_SOCKET_TIMEOUT;

    public AmazonS3FileTransfer(AWSCredentialsProvider credentialProvider,
                                 Regions region,
                                 String bucketName,
                                 boolean transferAcceleration,
                                 EncryptionType encryptionType,
                                 String clientSideBase64AesEncryptionKey,
                                 Protocol protocol,
                                 int connectionTimeout,
                                 int socketTimeout) throws Exception {
        this.credentialProvider = credentialProvider;
        this.region = region;
        this.bucketName = bucketName;
        this.transferAcceleration = transferAcceleration;
        this.encryptionType = encryptionType;
        this.clientSideBase64AesEncryptionKey = clientSideBase64AesEncryptionKey;
        this.connectionTimeout = connectionTimeout;
        this.socketTimeout = socketTimeout;

        String errMsg = validateProperties();
        if (StringUtils.isNotBlank(errMsg)) {
            throw new Exception("The AmazonS3FileTransfer was not configured properly with validation message: " + errMsg);
        }

        buildS3Transfer();
    }

    private String validateProperties() {
        if (this.credentialProvider == null) {
            return "Credential is not provided";
        }

        if (StringUtils.isBlank(this.bucketName)) {
            return "Bucket Name is not provided";
        }

        if (this.encryptionType != null && this.encryptionType == EncryptionType.CLIENT_SIDE_ENCRYPTION_MASTER_KEY
                && StringUtils.isBlank(this.clientSideBase64AesEncryptionKey)) {
            return "Client-side encryption customer-provided master key is null";
        }

        if (this.protocol == null) {
            return "Protocol must not be null";
        }

        return null;
    }

    private void buildS3Transfer() {
        if (this.encryptionType != null && this.encryptionType == EncryptionType.SERVER_SIDE_ENCRYPTION_S3_MANAGED) {
            sseAlgorithm = ObjectMetadata.AES_256_SERVER_SIDE_ENCRYPTION;
        }

        this.s3Client = buildS3Client();
    }


    private AmazonS3 buildS3Client() {
        // the default request protocol is HTTPS but set here to make sure
        ClientConfiguration clientConfiguration = new ClientConfiguration();
        clientConfiguration.setProtocol(this.protocol);
        clientConfiguration.setConnectionTimeout(connectionTimeout);
        clientConfiguration.setSocketTimeout(socketTimeout);

        if (this.encryptionType != null && this.encryptionType == EncryptionType.CLIENT_SIDE_ENCRYPTION_MASTER_KEY) {
            SecretKey mySymmetricKey = new SecretKeySpec(Base64.decode(this.clientSideBase64AesEncryptionKey), "AES");
            EncryptionMaterials encryptionMaterials = new EncryptionMaterials(mySymmetricKey);

            return AmazonS3EncryptionClientBuilder.standard()
                    .withAccelerateModeEnabled(this.transferAcceleration)
                    .withClientConfiguration(clientConfiguration)
                    .withCredentials(this.credentialProvider)
                    .withEncryptionMaterials(new StaticEncryptionMaterialsProvider(encryptionMaterials))
                    .withRegion(this.region)
                    .build();
        }

        return AmazonS3ClientBuilder.standard()
                .withAccelerateModeEnabled(this.transferAcceleration)
                .withClientConfiguration(clientConfiguration)
                .withCredentials(this.credentialProvider)
                .withRegion(this.region)
                .build();
    }

    public AmazonS3 getS3Client() {
        return s3Client;
    }

    public void downloadFile(String remoteFilePath, String localDestinationFilePath, boolean decompress, CompressionType compressionType, String password) throws IOException {
        InputStream is = null;
        InputStream compressIS = null;
        FileOutputStream fos = null;
        try {
            if (doesFileExist(remoteFilePath)) {
                GetObjectRequest getObjectRequest = new GetObjectRequest(this.bucketName, remoteFilePath);
                File localFile = new File(localDestinationFilePath);

                File parentDirectory = localFile.getParentFile();
                if (parentDirectory != null && !parentDirectory.exists()) {
                    parentDirectory.mkdirs();
                }

                s3Client.getObject(getObjectRequest, localFile);
                }
        } finally {
            safeCloseResouce(fos);
            safeCloseResouce(compressIS);

            safeCloseResouce(is);
        }
    }

    public boolean doesFileExist(String remoteFilePath) {
        return s3Client.doesObjectExist(this.bucketName, remoteFilePath);
    }


    /**
     * Builder class for building AmazonS3FileTransfer with options
     */
    public static class AmazonS3FileTransferBuilder {
        private String bucketName = null;
        private Regions region = null;
        private AWSCredentialsProvider credentialProvider = null;
        private EncryptionType encryptionType = null;;
        private String clientSideBase64AesEncryptionKey = null;
        private boolean transferAcceleration = false;
        private Protocol protocol = Protocol.HTTPS;
        private int connectionTimeout = ClientConfiguration.DEFAULT_CONNECTION_TIMEOUT;
        private int socketTimeout = ClientConfiguration.DEFAULT_SOCKET_TIMEOUT;

        public static AmazonS3FileTransferBuilder standard() {
            return new AmazonS3FileTransferBuilder();
        }

        public AmazonS3FileTransferBuilder withCredentials(String accessKeyId, String secretKey) {
            return withCredentials(new AWSStaticCredentialsProvider(new BasicAWSCredentials(accessKeyId, secretKey)));
        }

        public AmazonS3FileTransferBuilder withCredentials(AWSCredentialsProvider credentialProvider) {
            this.credentialProvider = credentialProvider;
            return this;
        }

        public AmazonS3FileTransferBuilder withRegion(Regions region) {
            this.region = region;
            return this;
        }

        public AmazonS3FileTransferBuilder withBucketName(String bucketName) {
            this.bucketName = bucketName;
            return this;
        }

        public AmazonS3FileTransferBuilder withTransferAcceleration(boolean transferAcceleration) {
            this.transferAcceleration = transferAcceleration;
            return this;
        }

        public AmazonS3FileTransferBuilder withEncryptionType(EncryptionType encryptionType) {
            this.encryptionType = encryptionType;
            return this;
        }

        public AmazonS3FileTransferBuilder withClientSideBase64AesEncryptionKey(String clientSideBase64AesEncryptionKey) {
            this.clientSideBase64AesEncryptionKey = clientSideBase64AesEncryptionKey;
            return this;
        }

        public AmazonS3FileTransferBuilder withProtocol(Protocol protocol) {
            this.protocol = protocol;
            return this;
        }

        public AmazonS3FileTransferBuilder withConnectionTimeout(int connectionTimeout) {
            this.connectionTimeout = connectionTimeout;
            return this;
        }

        public AmazonS3FileTransferBuilder withSocketTimeout(int socketTimeout) {
            this.socketTimeout = socketTimeout;
            return this;
        }

        public AmazonS3FileTransfer build() throws Exception {
            return new AmazonS3FileTransfer(this.credentialProvider,
                    this.region,
                    this.bucketName,
                    this.transferAcceleration,
                    this.encryptionType,
                    this.clientSideBase64AesEncryptionKey,
                    this.protocol,
                    this.connectionTimeout,
                    this.socketTimeout);
        }
    }

    public enum EncryptionType {
        /**
         * Protecting Data Using Server-Side Encryption with AWS KMS-Managed Keys (SSE-KMS)
         */
        // SERVER_SIDE_ENCRYPTION_KMS_MANAGED,
        /**
         * Protecting Data Using Server-Side Encryption with Amazon S3-Managed Encryption Keys (SSE-S3)
         */
        SERVER_SIDE_ENCRYPTION_S3_MANAGED,
        /**
         * Protecting Data Using Server-Side Encryption with Customer-Provided Encryption Keys (SSE-C)
         */
        // SERVER_SIDE_ENCRYPTION_CUSTOMER_PROVIDED,
        /**
         * Client-Side Encryption (Option 1: Using an AWS KMS-Managed Customer Master Key)
         */
        // CLIENT_SIDE_ENCRYPTION_KMS_MANAGED,
        /**
         * Client-Side Encryption (Option 2: Using a Client-Side Master Key)
         */
        CLIENT_SIDE_ENCRYPTION_MASTER_KEY;

    }
}
