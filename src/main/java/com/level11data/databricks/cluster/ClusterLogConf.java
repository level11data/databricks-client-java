package com.level11data.databricks.cluster;

import com.level11data.databricks.client.entities.clusters.ClusterLogConfDTO;

public class ClusterLogConf {
    public final DbfsStorageInfo DbfsInfo;
    public final S3StorageInfo S3Info;

    public ClusterLogConf(DbfsStorageInfo dbfsInfo) {
        DbfsInfo = dbfsInfo;
        S3Info = null;
    }

    public ClusterLogConf(S3StorageInfo s3Info) {
        DbfsInfo = null;
        S3Info = s3Info;
    }

    public ClusterLogConf(ClusterLogConfDTO logConfInfo) {
        if(logConfInfo.DBFS != null) {
            DbfsInfo = new DbfsStorageInfo(logConfInfo.DBFS.Destination);
            S3Info = null;
        } else if (logConfInfo.S3 != null){
            DbfsInfo = null;
            S3Info = new S3StorageInfo(logConfInfo.S3.Destination,
                    logConfInfo.S3.Region,
                    logConfInfo.S3.Endpoint,
                    logConfInfo.S3.EnableEncryption,
                    logConfInfo.S3.EncryptionType,
                    logConfInfo.S3.KmsKey,
                    logConfInfo.S3.CannedAcl);
        } else {
            DbfsInfo = null;
            S3Info = null;
        }
    }

    public StorageInfo getStorageInfo() {
        if(DbfsInfo != null) {
            return DbfsInfo;
        } else {
            return S3Info;
        }
    }

}
