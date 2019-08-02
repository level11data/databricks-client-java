package com.level11data.databricks.cluster;

import com.level11data.databricks.instancepool.InstancePool;

import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;

public interface Cluster {

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

    String getId();

    SparkVersion getSparkVersion();

    NodeType getDefaultNodeType();

    String getCreatorUserName();

    ServiceType getCreatedBy();

    ClusterSource getClusterSource();

    SparkNode getDriver();

    long getSparkContextId();

    /**
     *
     * @return Port number that JDBC services runs at.
     *   Returns null if not applicable
     *
     */
    Integer getJdbcPort();

    Date getStartTime();

    int getNumWorkers() throws ClusterConfigException;

    AutoScale getAutoScale();

    String getName();

    AwsAttributes getAwsAttributes();

    boolean getElasticDiskEnabled();

    Map<String, String> getSparkConf();

    List<String> getSshPublicKeys();

    Map<String, String> getDefaultTags();

    Map<String, String> getCustomTags();

    ClusterLogConf getClusterLogConf();

    Map<String, String> getSparkEnvironmentVariables();

    /**
     *
     * @return Number of minutes until auto termination.
     *   Returns zero if not set, or not applicable.
     */
    int getAutoTerminationMinutes();

    InstancePool getInstancePool();
}
