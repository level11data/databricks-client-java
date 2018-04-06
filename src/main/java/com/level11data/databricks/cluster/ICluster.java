package com.level11data.databricks.cluster;

import java.math.BigInteger;
import java.util.ArrayList;

public interface ICluster {

    ClusterState getState() throws ClusterConfigException;

    String getStateMessage() throws ClusterConfigException;

    ArrayList<SparkNode> getExecutors() throws ClusterConfigException;

    BigInteger getTerminatedTime() throws ClusterConfigException;

    BigInteger getLastStateLossType() throws ClusterConfigException;

    BigInteger getLastActivityTime() throws ClusterConfigException;

    BigInteger getClusterMemoryMb() throws ClusterConfigException;

    BigInteger getClusterCores() throws ClusterConfigException;

    LogSyncStatus getLogStatus() throws ClusterConfigException;

    TerminationReason getTerminationReason() throws ClusterConfigException;

}
