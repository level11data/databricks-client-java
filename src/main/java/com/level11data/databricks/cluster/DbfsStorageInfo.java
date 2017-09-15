package com.level11data.databricks.cluster;

public class DbfsStorageInfo extends StorageInfo {
    public final String Destination;

    public DbfsStorageInfo(String destination){
        Destination = destination;
    }
}
