package com.level11data.databricks.cluster;

public class DbfsStorageInfo extends AbstractStorageInfo {
    public final String Destination;

    public DbfsStorageInfo(String destination){
        Destination = destination;
    }
}
