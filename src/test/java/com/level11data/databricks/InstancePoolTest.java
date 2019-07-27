package com.level11data.databricks;

import com.level11data.databricks.cluster.AwsAttribute.EbsVolumeType;
import com.level11data.databricks.cluster.ClusterSpec;
import com.level11data.databricks.cluster.ClusterState;
import com.level11data.databricks.cluster.InteractiveCluster;
import com.level11data.databricks.config.DatabricksClientConfiguration;
import com.level11data.databricks.instancepool.AwsAvailability;
import com.level11data.databricks.instancepool.InstancePool;
import com.level11data.databricks.instancepool.InstancePoolConfigException;
import com.level11data.databricks.job.AutomatedJarJob;
import com.level11data.databricks.job.AutomatedNotebookJob;
import com.level11data.databricks.job.Job;
import com.level11data.databricks.job.JobConfigException;
import com.level11data.databricks.job.run.AutomatedJarJobRun;
import com.level11data.databricks.job.run.JobRun;
import com.level11data.databricks.job.run.RunResultState;
import com.level11data.databricks.library.JarLibrary;
import com.level11data.databricks.session.WorkspaceSession;
import com.level11data.databricks.workspace.Notebook;
import org.junit.Assert;
import org.junit.Test;

import java.io.File;
import java.net.URI;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashMap;
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
    public void testModifyInstancePool() throws Exception {
        //Set pool name to ClassName.MethodName TIMESTAMP
        long now = System.currentTimeMillis();
        String poolName = this.getClass().getSimpleName() + "." +
                Thread.currentThread().getStackTrace()[1].getMethodName() +
                " " +now;

        //create pool with bare minimum properties
        InstancePool instancePool = _databricks.createInstancePool()
                .withName(poolName)
                .withNodeTypeId(NODE_TYPE)
                .create();

        //modify pool (just min instances)
        InstancePool modifiedInstancePool = instancePool.edit().withMinIdleInstances(1).modify();

        Assert.assertEquals("Modified InstancePool MinIdleInstances does not match new value",
                1, _databricks.getInstancePool(modifiedInstancePool.getId()).getMinIdleInstances());

        //cleanup
        modifiedInstancePool.delete();
    }

    @Test
    public void testAwsModifyInstancePool() throws Exception {
        //Set pool name to ClassName.MethodName TIMESTAMP
        long now = System.currentTimeMillis();
        String poolName = this.getClass().getSimpleName() + "." +
                Thread.currentThread().getStackTrace()[1].getMethodName() +
                " " +now;

        //create pool with bare minimum properties
        InstancePool instancePool = _databricks.createInstancePool()
                .withName(poolName)
                .withNodeTypeId("c4.2xlarge") //use instance type which requires EBS volume
                .withDiscSpec(EbsVolumeType.GENERAL_PURPOSE_SSD, 1, 100)
                .create();

        //modify pool (just min instances)
        InstancePool modifiedInstancePool = instancePool.edit().withMinIdleInstances(1).modify();

        Assert.assertEquals("Modified InstancePool MinIdleInstances does not match new value",
                1, _databricks.getInstancePool(modifiedInstancePool.getId()).getMinIdleInstances());

        //cleanup
        modifiedInstancePool.delete();
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
                instancePool.getDiskSpec());

        Assert.assertNull("Default InstancePool config EbsVolumeSize NOT null",
                instancePool.getDiskSpec());

        Assert.assertNull("Default InstancePool config EbsVolumeType NOT null",
                instancePool.getDiskSpec());

        Assert.assertNull("Default InstancePool config InstanceProfileARN NOT null",
                instancePool.getAwsAttributes().InstanceProfileARN);

        Assert.assertEquals("Default InstancePool AwsAvailability is NOT SPOT",
                AwsAvailability.SPOT, instancePool.getAwsAttributes().Availability);

        Assert.assertEquals("Default InstancePool SpotBidPricePercent is NOT 100%",
                100, instancePool.getAwsAttributes().SpotBidPricePercent
                        .toUnsignedLong(instancePool.getAwsAttributes().SpotBidPricePercent));

        Assert.assertNotNull("Default InstancePool ZoneId is NULL", instancePool.getAwsAttributes().ZoneId);

        //modify InstancePool
        InstancePool modifiedInstancePool = instancePool.edit()
                .withIdleInstanceAutoTerminationMinutes(10)
                .withMinIdleInstances(1)
                .withMaxCapacity(2)
                .withName(instancePool + " new name")
                .modify();

        Assert.assertEquals("Modified AutoTerminationMinutes does not equal modified value",
                10, modifiedInstancePool.getIdleInstanceAutoTerminationMinutes().intValue());

        Assert.assertEquals("Modified MinIdleInstances does not equal modified value",
                1, modifiedInstancePool.getMinIdleInstances());

        Assert.assertEquals("Modified MaxCapacity does not equal modified value",
                2, modifiedInstancePool.getMaxCapacity().intValue());

        Assert.assertEquals("Modified Name does not equal modified value",
                instancePool + " new name", modifiedInstancePool.getName());

        InstancePool retrievedInstancePoolByName = _databricks.getFirstInstancePoolByName(instancePool + " new name");

        Assert.assertEquals("Retrieved Instance Pool (by name) does not match Id",
                instancePool.getId(), retrievedInstancePoolByName.getId());

        InstancePool retrievedInstancePoolById = _databricks.getInstancePool(instancePool.getId());

        Assert.assertEquals("Retrieved Instance Pool (by id) does not match Name",
                instancePool + " new name", modifiedInstancePool.getName());

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
                .withDiscSpec(EbsVolumeType.GENERAL_PURPOSE_SSD, 1, 100)
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
                .withDiscSpec(EbsVolumeType.GENERAL_PURPOSE_SSD, 1, 100)
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

    @Test
    public void testResizeInstancePool() throws Exception {
        long now = System.currentTimeMillis();
        ClassLoader loader = Thread.currentThread().getContextClassLoader();

        //Set pool name to ClassName.MethodName TIMESTAMP
        String uniqueName = this.getClass().getSimpleName() + "." +
                Thread.currentThread().getStackTrace()[1].getMethodName() +
                "-" + now;

        InstancePool instancePool = _databricks.createInstancePool()
                .withName(uniqueName)
                .withNodeTypeId(NODE_TYPE)
                .withIdleInstanceAutoTerminationMinutes(5)
                .create();

        Assert.assertEquals("InstancePool does NOT have expected MinIdleInstances",
                0, instancePool.getMinIdleInstances());

        //modify instance pool to increase min number of instances
        InstancePool modifiedInstancePool = instancePool.edit()
                .withMinIdleInstances(2)
                .modify();

        boolean instancesActive = false;
        long secondsWaited = 0;
        while(instancesActive == false) {
            int idleCount = modifiedInstancePool.getStats().IdleCount;
            int usedCount = modifiedInstancePool.getStats().UsedCount;
            int pendingIdleCount = modifiedInstancePool.getStats().PendingIdleCount;
            int pendingUsedCount = modifiedInstancePool.getStats().PendingUsedCount;

            System.out.println("Time (sec)="+secondsWaited +
              ", IdleCount=" + idleCount +
              ", UsedCount=" + usedCount +
              ", PendingIdleCount=" + pendingIdleCount +
              ", PendingUsedCount=" + pendingUsedCount);

            long millisecondsToWait = 10000; //10 seconds

            if(idleCount > 0) {
                instancesActive = true;
            }

            Thread.sleep(millisecondsToWait);

            secondsWaited = secondsWaited + (millisecondsToWait / 1000);
        }

// InstancePool needs to wait 30 seconds before PendingIdleCount changes
//   waiting on STATUS field to be finalized
//        Assert.assertEquals("Modified Instance Pool does not have any pending instances",
//                2, modifiedInstancePool.getStats().PendingIdleCount);



        //cleanup
        modifiedInstancePool.delete();
    }
}
