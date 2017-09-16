package com.level11data.databricks.cluster;

import com.level11data.databricks.entities.clusters.LogSyncStatusDTO;

import java.math.BigInteger;

public class LogSyncStatus {
    public final BigInteger LastAttempted;
    public final String LastException;

    public LogSyncStatus(LogSyncStatusDTO statusInfo) {
        LastAttempted = statusInfo.LastAttempted;
        LastException = statusInfo.LastException;
    }
}
