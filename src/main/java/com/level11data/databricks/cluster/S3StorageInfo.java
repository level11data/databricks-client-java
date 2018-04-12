package com.level11data.databricks.cluster;

public class S3StorageInfo extends AbstractStorageInfo {
    public final String Destination;
    public final String Region;
    public final String Endpoint;
    public final Boolean EnableEncryption;
    public final String EncryptionType;
    public final String KmsKey;
    public final String CannedAcl;

    public S3StorageInfo(String destination,
            String region,
            String endpoint,
            Boolean enableEncryption,
            String encryptionType,
            String kmsKey,
            String cannedAcl) {
        Destination = destination;
        Region = region;
        Endpoint = endpoint;
        EnableEncryption = enableEncryption;
        EncryptionType = encryptionType;
        KmsKey = kmsKey;
        CannedAcl = cannedAcl;
    }
}
