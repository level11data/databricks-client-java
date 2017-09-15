package com.level11data.databricks.cluster;

import java.math.BigInteger;

public class LogSyncStatus {
    public final BigInteger LastAttempted;
    public final String LastException;

    public LogSyncStatus(com.level11data.databricks.entities.clusters.LogSyncStatus statusInfo) {
        LastAttempted = statusInfo.LastAttempted;
        LastException = statusInfo.LastException;
    }
}
