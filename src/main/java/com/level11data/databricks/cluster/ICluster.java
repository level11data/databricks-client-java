package com.level11data.databricks.cluster;

import com.level11data.databricks.client.HttpException;
import com.level11data.databricks.client.entities.clusters.TerminationReasonDTO;

import java.math.BigInteger;
import java.util.ArrayList;

public interface ICluster {

    ClusterState getState() throws HttpException;

    String getStateMessage() throws HttpException;

    ArrayList<SparkNode> getExecutors() throws HttpException, ClusterConfigException;

    BigInteger getTerminatedTime() throws HttpException;

    BigInteger getLastStateLossType() throws HttpException;

    BigInteger getLastActivityTime() throws HttpException;

    BigInteger getClusterMemoryMb() throws HttpException;

    BigInteger getClusterCores() throws HttpException;

    LogSyncStatus getLogStatus() throws HttpException;

    TerminationReasonDTO getTerminationReason() throws HttpException;

}
