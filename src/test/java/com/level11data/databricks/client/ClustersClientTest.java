package com.level11data.databricks.client;


import com.level11data.databricks.DatabricksSession;
import com.level11data.databricks.cluster.Cluster;
import com.level11data.databricks.config.DatabricksClientConfiguration;
import com.level11data.databricks.entities.clusters.ClusterInfo;
import org.junit.Test;
import org.junit.Assert;

import javax.ws.rs.core.UriBuilder;
import java.io.InputStream;
import java.net.URI;

public class ClustersClientTest {
    public static final String CLIENT_CONFIG_RESOURCE_NAME = "test.properties";

    ClassLoader loader = Thread.currentThread().getContextClassLoader();
    InputStream resourceStream = loader.getResourceAsStream(CLIENT_CONFIG_RESOURCE_NAME);
    DatabricksSession _databricks;
    DatabricksClientConfiguration _databricksConfig;

    public ClustersClientTest() throws Exception {
        loadConfigFromResource();
    }

    private void loadConfigFromResource() throws Exception {
        if(resourceStream == null) {
            throw new IllegalArgumentException("Resource Not Found: " + CLIENT_CONFIG_RESOURCE_NAME);
        }
        _databricksConfig = new DatabricksClientConfiguration(resourceStream);

        _databricks = new DatabricksSession(_databricksConfig);
    }

    @Test
    public void testCreateSimpleClusterFixedSize() throws Exception {
        long now = System.currentTimeMillis();
        String clusterName = "test cluster fixed size " + now;
        int numberOfExecutors = 1;

        Cluster cluster = _databricks.createCluster(clusterName, numberOfExecutors)
                .withAutoTerminationMinutes(20).create();

        Assert.assertEquals("Simple Fixed Size Cluster Name does NOT match expected Name",
                clusterName, cluster.Name);

        String clusterId = cluster.Id;

        Assert.assertNotNull("Simple Fixed Size Cluster Id IS NULL", clusterId);

        Assert.assertEquals("Simple Fixed Size Cluster did NOT enter a PENDING state after create",
                ClusterInfo.ClusterState.PENDING, cluster.getState());

        while(cluster.getState() == ClusterInfo.ClusterState.PENDING) {
          //wait until cluster is properly started
          // should not take more than 100 seconds from a cold start
          Thread.sleep(10000); //wait 10 seconds
        }

        Assert.assertEquals("Simple Fixed Size Cluster did NOT enter a RUNNING state after create",
                ClusterInfo.ClusterState.RUNNING, cluster.getState());

        //TODO Have the API match the Docs and return "executors" in the payload
        //Assert.assertEquals("Simple Fixed Size Cluster was NOT created with expected number of executors",
        //        numberOfExecutors, cluster.getExecutors().length);

        //TODO Change the Default Spark Version from "Spark 1.6.2 (Hadoop 1)"
        Assert.assertEquals("Simple Fixed Size Cluster Spark Version does NOT match default",
                _databricks.getDefaultSparkVersion(), cluster.SparkVersion);

        //TODO Change the Default Node Type from "Memory Optimized"
        Assert.assertEquals("Simple Fixed Size Cluster Node Type does NOT match default",
                _databricks.getDefaultNodeType(), cluster.NodeType);

        //TODO Change the Default Node Type from "Memory Optimized"
        Assert.assertEquals("Simple Fixed Size Cluster Driver Node Type does NOT match default",
                _databricks.getDefaultNodeType(), cluster.DriverNodeType);

        cluster.restart();

        Assert.assertEquals("Simple Fixed Size Cluster did NOT enter a RESTARTING state after restart",
                ClusterInfo.ClusterState.RESTARTING, cluster.getState());

        while(cluster.getState() == ClusterInfo.ClusterState.RESTARTING) {
            Thread.sleep(5000); //wait 5 seconds
        }

        Assert.assertEquals("Simple Fixed Size Cluster did NOT enter a RUNNING state after restart",
                ClusterInfo.ClusterState.RUNNING, cluster.getState());

        numberOfExecutors = 0;
        cluster = cluster.resize(numberOfExecutors);

        Assert.assertEquals("Simple Fixed Size Cluster did NOT enter a RESIZING state after resize",
                ClusterInfo.ClusterState.RESIZING, cluster.getState());

        while(cluster.getState() == ClusterInfo.ClusterState.RESIZING) {
            Thread.sleep(5000); //wait 5 seconds
        }

        Assert.assertEquals("Simple Fixed Size Cluster did NOT enter a RUNNING state after resize",
                ClusterInfo.ClusterState.RUNNING, cluster.getState());

        //TODO Have the API match the Docs and return "executors" in the payload
        //Assert.assertEquals("Simple Fixed Size Cluster was NOT resized with expected number of executors",
        //        numberOfExecutors, cluster.getExecutors().length);

        cluster.terminate();

        Assert.assertEquals("Simple Fixed Size Cluster did NOT enter a TERMINATING state after terminate",
                ClusterInfo.ClusterState.TERMINATING, cluster.getState());

        while(cluster.getState() == ClusterInfo.ClusterState.TERMINATING) {
            Thread.sleep(5000); //wait 5 seconds
        }

        Assert.assertEquals("Simple Fixed Size Cluster did NOT enter a TERMINATED state after terminate",
                ClusterInfo.ClusterState.TERMINATED, cluster.getState());
    }

    @Test
    public void testCreateSimpleClusterAutoscaling() throws Exception {
        long now = System.currentTimeMillis();
        String clusterName = "test cluster autoscaling " + now;
        Integer minWorkers = 0;
        Integer maxWorkers = 1;

        Cluster cluster = _databricks.createCluster(clusterName, minWorkers, maxWorkers)
                .withAutoTerminationMinutes(20).create();

        Assert.assertEquals("Simple Autoscaling Cluster Name does NOT match expected NAME",
                clusterName, cluster.Name);

        String clusterId = cluster.Id;

        Assert.assertNotNull("Simple Autoscaling Cluster Id is NULL", clusterId);

        Assert.assertEquals("Simple Autoscaling Cluster did NOT enter a PENDING state after create",
                ClusterInfo.ClusterState.PENDING, cluster.getState());

        while(cluster.getState() == ClusterInfo.ClusterState.PENDING) {
            //wait until cluster is properly started
            // should not take more than 100 seconds from a cold start
            Thread.sleep(10000); //wait 10 seconds
        }

        Assert.assertEquals("Simple Autoscaling Cluster did NOT enter a RUNNING state after create",
                ClusterInfo.ClusterState.RUNNING, cluster.getState());

        Assert.assertEquals("Simple Autoscaling Cluster was NOT created with expected MINIMUM number of workers",
                cluster.AutoScale.MinWorkers, minWorkers);

        Assert.assertEquals("Simple Autoscaling Cluster was NOT created with expected MAXIMUM number of workers",
                cluster.AutoScale.MaxWorkers, maxWorkers);

        //TODO Change the Default Spark Version from "Spark 1.6.2 (Hadoop 1)"
        Assert.assertEquals("Simple Autoscaling Cluster Spark Version does NOT match default",
                _databricks.getDefaultSparkVersion(), cluster.SparkVersion);

        //TODO Change the Default Node Type from "Memory Optimized"
        Assert.assertEquals("Simple Autoscaling Cluster Node Type does NOT match default",
                _databricks.getDefaultNodeType(), cluster.NodeType);

        //TODO Change the Default Node Type from "Memory Optimized"
        Assert.assertEquals("Simple Autoscaling Cluster Driver Node Type does NOT match default",
                _databricks.getDefaultNodeType(), cluster.DriverNodeType);

        cluster.restart();

        Assert.assertEquals("Simple Autoscaling Cluster did NOT enter a RESTARTING state after restart",
                ClusterInfo.ClusterState.RESTARTING, cluster.getState());

        while(cluster.getState() == ClusterInfo.ClusterState.RESTARTING) {
            Thread.sleep(5000); //wait 5 seconds
        }

        Assert.assertEquals("Simple Autoscaling Cluster did NOT enter a RUNNING state after restart",
                ClusterInfo.ClusterState.RUNNING, cluster.getState());

        minWorkers = 1;
        maxWorkers = 2;
        cluster = cluster.resize(minWorkers, maxWorkers);

        Assert.assertEquals("Simple Autoscaling Cluster did NOT enter a RESIZING state after resize",
                ClusterInfo.ClusterState.RESIZING, cluster.getState());

        while(cluster.getState() == ClusterInfo.ClusterState.RESIZING) {
            Thread.sleep(5000); //wait 5 seconds
        }

        Assert.assertEquals("Simple Autoscaling Cluster did NOT enter a RUNNING state after resize",
                ClusterInfo.ClusterState.RUNNING, cluster.getState());

        Assert.assertEquals("Simple Autoscaling Cluster was NOT resized with expected MINIMUM number of workers",
                cluster.AutoScale.MinWorkers, minWorkers);

        Assert.assertEquals("Simple Autoscaling Cluster was NOT resized with expected MAXIMUM number of workers",
                cluster.AutoScale.MaxWorkers, maxWorkers);

        cluster.terminate();

        Assert.assertEquals("Simple Autoscaling Cluster did NOT enter a TERMINATING state after terminate",
                ClusterInfo.ClusterState.TERMINATING, cluster.getState());

        while(cluster.getState() == ClusterInfo.ClusterState.TERMINATING) {
            Thread.sleep(5000); //wait 5 seconds
        }

        Assert.assertEquals("Simple Autoscaling Cluster did NOT enter a TERMINATED state after terminate",
                ClusterInfo.ClusterState.TERMINATED, cluster.getState());


    }


}
