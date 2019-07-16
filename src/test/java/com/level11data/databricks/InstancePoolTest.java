package com.level11data.databricks;

import com.level11data.databricks.cluster.AwsAttribute.EbsVolumeType;
import com.level11data.databricks.cluster.ClusterSpec;
import com.level11data.databricks.cluster.ClusterState;
import com.level11data.databricks.cluster.InteractiveCluster;
import com.level11data.databricks.config.DatabricksClientConfiguration;
import com.level11data.databricks.instancepool.AwsAvailability;
import com.level11data.databricks.instancepool.InstancePool;
import com.level11data.databricks.job.AutomatedJarJob;
import com.level11data.databricks.job.run.AutomatedJarJobRun;
import com.level11data.databricks.job.run.RunResultState;
import com.level11data.databricks.library.JarLibrary;
import com.level11data.databricks.session.WorkspaceSession;
import org.junit.Assert;
import org.junit.Test;

import java.io.File;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;

public class InstancePoolTest {
    //load config from default resource databricks-client.properties (in test/resources)
    DatabricksClientConfiguration _databricksConfig = new DatabricksClientConfiguration();

    public final String DBR_VERSION = _databricksConfig
            .getString("com.level11data.databricks.client.default.cluster.sparkVersion");

    public final String NODE_TYPE = _databricksConfig
            .getString("com.level11data.databricks.client.default.cluster.nodeType");

    WorkspaceSession _databricks = new WorkspaceSession(_databricksConfig);

    public InstancePoolTest() throws Exception {

    }

    @Test
    public void testDefaultInstancePool() throws Exception {
        long now = System.currentTimeMillis();


        //Set pool name to ClassName.MethodName TIMESTAMP
        String poolName = this.getClass().getSimpleName() + "." +
                Thread.currentThread().getStackTrace()[1].getMethodName() +
                " " +now;

        //create pool with bare minimum properties
        InstancePool instancePool = _databricks.createInstancePool()
                .withName(poolName)
                .withNodeTypeId(NODE_TYPE)
                .create();

        Assert.assertEquals("Default InstancePool config MinIdleInstances NOT zero",
                0, instancePool.getMinIdleInstances());

        Assert.assertNull("Default InstancePool config MaxCapacity NOT null", instancePool.getMaxCapacity());

        Assert.assertEquals("Default InstancePool config IdleCount NOT zero",
                0, instancePool.getStats().IdleCount);

        Assert.assertEquals("Default InstancePool config PendingIdleCount NOT zero",
                0, instancePool.getStats().PendingIdleCount);

        Assert.assertEquals("Default InstancePool config PendingUsedCount NOT zero",
                0, instancePool.getStats().PendingUsedCount);

        Assert.assertEquals("Default InstancePool config UsedCount NOT zero",
                0, instancePool.getStats().UsedCount);

        Assert.assertEquals("Default InstancePool config ElasticStorate NOT false",
                false, instancePool.isElasticDiskEnabled());

        Assert.assertEquals("InstancePool NodeType does NOT match",
                NODE_TYPE, instancePool.getNodeType().InstanceTypeId);

        Assert.assertNull("Default InstancePool config EbsVolumeCount NOT null",
                instancePool.getAwsAttributes().EbsVolumeCount);

        Assert.assertNull("Default InstancePool config EbsVolumeSize NOT null",
                instancePool.getAwsAttributes().EbsVolumeSize);

        Assert.assertNull("Default InstancePool config EbsVolumeType NOT null",
                instancePool.getAwsAttributes().EbsVolumeType);

        Assert.assertNull("Default InstancePool config InstanceProfileARN NOT null",
                instancePool.getAwsAttributes().InstanceProfileARN);

        Assert.assertEquals("Default InstancePool AwsAvailability is NOT SPOT",
                AwsAvailability.SPOT, instancePool.getAwsAttributes().Availability);

        Assert.assertEquals("Default InstancePool SpotBidPricePercent is NOT 100%",
                100, instancePool.getAwsAttributes().SpotBidPricePercent
                        .toUnsignedLong(instancePool.getAwsAttributes().SpotBidPricePercent));

        Assert.assertNotNull("Default InstancePool ZoneId is NULL", instancePool.getAwsAttributes().ZoneId);

        //cleanup
        instancePool.delete();
    }

    @Test
    public void testInstancePoolInteractiveCluster() throws Exception {
        long now = System.currentTimeMillis();


        //Set pool name to ClassName.MethodName TIMESTAMP
        String poolName = this.getClass().getSimpleName() + "." +
                Thread.currentThread().getStackTrace()[1].getMethodName() +
                " " + now;

        //create pool and then interactive cluster
        InteractiveCluster cluster = _databricks.createInstancePool()
                .withName(poolName)
                .withNodeTypeId(NODE_TYPE)
                .withMinIdleInstances(2)
                .withMaxCapacity(2)
                .withPreloadedSparkVersion(DBR_VERSION)
                .withIdleInstanceAutoTerminationMinutes(10)
                .withAwsEbsVolume(EbsVolumeType.GENERAL_PURPOSE_SSD, 1, 100)
                .withAwsZone("us-west-2a")
                .withAwsSpotBidPricePercent(99)
                .withAwsAvailability(AwsAvailability.SPOT)
                .create()
                .createCluster(poolName, 1)
                .withSparkVersion(DBR_VERSION)
                .withAutoTerminationMinutes(10)
                .withAwsEbsVolume(EbsVolumeType.GENERAL_PURPOSE_SSD, 1, 100)
                .withAwsZone("us-west-2b")
                .withAwsSpotBidPricePercent(100)
                .withAwsAvailability(com.level11data.databricks.cluster.AwsAttribute.AwsAvailability.SPOT_WITH_FALLBACK)
                .withAwsFirstOnDemand(0)
                .create();

        Assert.assertEquals("InstancePool does NOT have expected MinIdleInstances",
                2, cluster.getInstancePool().getMinIdleInstances());

        Assert.assertEquals("InstancePool does NOT have expected MinIdleInstances",
                2, cluster.getInstancePool().getMaxCapacity().intValue());

        Assert.assertEquals("InstancePool does NOT have expected PreloadedSparkVersion",
                DBR_VERSION, cluster.getInstancePool().getPreloadedSparkVersions().get(0).Key);

        Assert.assertEquals("InstancePool InteractiveCluster did NOT enter a PENDING state after create",
                ClusterState.PENDING, cluster.getState());

        while(cluster.getState() == ClusterState.PENDING) {
            //wait until cluster is properly started
            // should not take more than 100 seconds from a cold start
            Thread.sleep(10000); //wait 10 seconds
        }

        Assert.assertEquals("InstancePool InteractiveCluster did NOT enter a RUNNING state after create",
                ClusterState.RUNNING, cluster.getState());

        //cleanup
        cluster.terminate();
        cluster.getInstancePool().delete();
    }

    @Test
    public void testInstancePoolAutomatedJob() throws Exception {
        long now = System.currentTimeMillis();
        String SIMPLE_JAR_RESOURCE_NAME = "simple-scala-spark-app_2.11-0.0.1.jar";
        ClassLoader loader = Thread.currentThread().getContextClassLoader();
        String localPath = loader.getResource(SIMPLE_JAR_RESOURCE_NAME).getFile();
        File jarFile = new File(localPath);


        //Set pool name to ClassName.MethodName TIMESTAMP
        String uniqueName = this.getClass().getSimpleName() + "." +
                Thread.currentThread().getStackTrace()[1].getMethodName() +
                "-" + now;

        String dbfsPath = "dbfs:/tmp/test/" + uniqueName + "/" + SIMPLE_JAR_RESOURCE_NAME;
        JarLibrary jarLibrary = _databricks.getJarLibrary(new URI(dbfsPath));

        List<String> baseParams = new ArrayList<String>();
        baseParams.add("hello");
        baseParams.add("world");

        //create pool and then automated cluster spec
        ClusterSpec clusterSpec = _databricks.createInstancePool()
                .withName(uniqueName)
                .withNodeTypeId(NODE_TYPE)
                .withMinIdleInstances(2)
                .withMaxCapacity(2)
                .withPreloadedSparkVersion(DBR_VERSION)
                .withIdleInstanceAutoTerminationMinutes(10)
                .create()
                .createClusterSpec(1)
                .withNodeType(NODE_TYPE)
                .withSparkVersion(DBR_VERSION)
                .createClusterSpec();

        AutomatedJarJob job = _databricks.createJob(jarLibrary,
                "com.level11data.example.scala.simpleapp.SimpleApp",
                jarFile,
                baseParams)
                .withName(uniqueName)
                .withClusterSpec(clusterSpec)
                .create();

        AutomatedJarJobRun run = job.run();

        while(!run.getRunState().LifeCycleState.isFinal()) {
            Thread.sleep(10000); //sleep 10 seconds
        }

        Assert.assertEquals("Job Run was NOT Successful", RunResultState.SUCCESS,
                run.getRunState().ResultState);

        Assert.assertEquals("JobRun InsancePool does NOT match InstancePool on Job",
                job.getClusterSpec().getInstancePoolId(), run.getCluster().getInstancePool().getId());

        //cleanup
        job.delete();
        run.getCluster().getInstancePool().delete();
        _databricks.deleteDbfsObject(dbfsPath, true);
    }

    @Test
    public void testAwsInstancePool() throws Exception {
        long now = System.currentTimeMillis();
        String SIMPLE_JAR_RESOURCE_NAME = "simple-scala-spark-app_2.11-0.0.1.jar";
        ClassLoader loader = Thread.currentThread().getContextClassLoader();

        //Set pool name to ClassName.MethodName TIMESTAMP
        String uniqueName = this.getClass().getSimpleName() + "." +
                Thread.currentThread().getStackTrace()[1].getMethodName() +
                "-" + now;

        InstancePool instancePool = _databricks.createInstancePool()
                .withName(uniqueName)
                .withNodeTypeId("c4.2xlarge")
                .withMinIdleInstances(2)
                .withMaxCapacity(2)
                .withPreloadedSparkVersion(DBR_VERSION)
                .withIdleInstanceAutoTerminationMinutes(5)
                .withAwsEbsVolume(EbsVolumeType.GENERAL_PURPOSE_SSD, 1, 100)
                .create();

        Assert.assertEquals("InstancePool does NOT have expected MinIdleInstances",
                2, instancePool.getMinIdleInstances());

        Assert.assertEquals("InstancePool does NOT have expected MinIdleInstances",
                2, instancePool.getMaxCapacity().intValue());

        Assert.assertEquals("InstancePool does NOT have expected PreloadedSparkVersion",
                DBR_VERSION, instancePool.getPreloadedSparkVersions().get(0).Key);


        //cleanup
        instancePool.delete();
    }
}
