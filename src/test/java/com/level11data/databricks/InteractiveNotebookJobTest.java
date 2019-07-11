package com.level11data.databricks;

import com.level11data.databricks.session.WorkspaceSession;
import com.level11data.databricks.cluster.ClusterState;
import com.level11data.databricks.cluster.InteractiveCluster;
import com.level11data.databricks.config.DatabricksClientConfiguration;
import com.level11data.databricks.job.InteractiveNotebookJob;
import com.level11data.databricks.job.run.InteractiveNotebookJobRun;
import com.level11data.databricks.util.TestUtils;
import com.level11data.databricks.workspace.ScalaNotebook;
import org.junit.Assert;
import org.junit.Test;

import java.io.File;
import java.util.HashMap;

public class InteractiveNotebookJobTest {

    //load config from default resource databricks-client.properties (in test/resources)
    DatabricksClientConfiguration _databricksConfig = new DatabricksClientConfiguration();

    WorkspaceSession _databricks = new WorkspaceSession(_databricksConfig);

    public final String DBR_VERSION = _databricksConfig
            .getString("com.level11data.databricks.client.default.cluster.sparkVersion");

    public final String NODE_TYPE = _databricksConfig
            .getString("com.level11data.databricks.client.default.cluster.nodeType");

    public static final String SIMPLE_SCALA_SOURCE_NOTEBOOK_RESOURCE_NAME = "test-notebook.scala";
    public static final String SIMPLE_SCALA_PARAMETERS_SOURCE_NOTEBOOK_RESOURCE_NAME = "test-notebook-parameters.scala";

    public InteractiveNotebookJobTest() throws Exception {

    }

    @Test
    public void testSimpleInteractiveNotebookJob() throws Exception {
        long now = System.currentTimeMillis();

        //Set cluster name to ClassName.MethodName TIMESTAMP
        String clusterName = this.getClass().getSimpleName() + "." +
                Thread.currentThread().getStackTrace()[1].getMethodName() +
                " " +now;

        //Create Interactive Cluster
        InteractiveCluster cluster = _databricks.createInteractiveCluster(clusterName, 1)
                .withAutoTerminationMinutes(20)
                .withSparkVersion(DBR_VERSION)
                .withNodeType(NODE_TYPE)
                .create();

        while(cluster.getState() == ClusterState.PENDING) {
            //wait until cluster is properly started
            // should not take more than 100 seconds from a cold start
            Thread.sleep(10000); //wait 10 seconds
        }

        //create notebook
        File localFile = TestUtils.getResourceByName(SIMPLE_SCALA_SOURCE_NOTEBOOK_RESOURCE_NAME);
        String workspacePath = "/tmp/test/" + clusterName;
        String workspaceNotebookPath = workspacePath + "/" + SIMPLE_SCALA_SOURCE_NOTEBOOK_RESOURCE_NAME;
        ScalaNotebook scalaNotebook = _databricks.createScalaNotebook(localFile).create(workspaceNotebookPath);

        //create job
        InteractiveNotebookJob job = cluster.createJob(scalaNotebook)
                .withName("testSimpleInteractiveNotebookJob "+now)
                .create();

        Assert.assertEquals("Job CreatorUserName does not equal " + _databricksConfig.getWorkspaceUsername(),
                _databricksConfig.getWorkspaceUsername(), job.getCreatorUserName());

        Assert.assertEquals("Job Parameters is not zero", 0, job.getBaseParameters().size());

        //run job
        InteractiveNotebookJobRun jobRun = job.run();

        Assert.assertEquals("Job Run CreatorUserName does not equal " + _databricksConfig.getWorkspaceUsername(),
                _databricksConfig.getWorkspaceUsername(), jobRun.getCreatorUserName());

        Assert.assertEquals("Job Run Override is not zero", 0, jobRun.getOverridingParameters().size());

        while(!jobRun.getRunState().LifeCycleState.isFinal()) {
            Thread.sleep(5000); //wait 5 seconds
        }

        Assert.assertEquals("Job Run Output Does Not Match", "2", jobRun.getOutput());

        //cleanup
        job.delete();
        cluster.terminate();
        scalaNotebook.delete();
    }

    @Test
    public void testSimpleInteractiveNotebookJobWithParams() throws Exception {
        long now = System.currentTimeMillis();


        //Set cluster name to ClassName.MethodName TIMESTAMP
        String clusterName = this.getClass().getSimpleName() + "." +
                Thread.currentThread().getStackTrace()[1].getMethodName() +
                " " +now;

        //Create Interactive Cluster
        InteractiveCluster cluster = _databricks.createInteractiveCluster(clusterName, 1)
                .withAutoTerminationMinutes(20)
                .withSparkVersion(DBR_VERSION)
                .withNodeType(NODE_TYPE)
                .create();

        while(cluster.getState() == ClusterState.PENDING) {
            //wait until cluster is properly started
            // should not take more than 100 seconds from a cold start
            Thread.sleep(10000); //wait 10 seconds
        }

        //create notebook
        File localFile = TestUtils.getResourceByName(SIMPLE_SCALA_PARAMETERS_SOURCE_NOTEBOOK_RESOURCE_NAME);
        String workspacePath = "/tmp/test/" + clusterName;
        String workspaceNotebookPath = workspacePath + "/" + SIMPLE_SCALA_PARAMETERS_SOURCE_NOTEBOOK_RESOURCE_NAME;
        ScalaNotebook scalaNotebook = _databricks.createScalaNotebook(localFile).create(workspaceNotebookPath);

        //create job
        HashMap<String,String> parameters = new HashMap<String,String>();
        parameters.put("parameter1", "Hello");
        parameters.put("parameter2", "World");

        InteractiveNotebookJob job = cluster.createJob(scalaNotebook, parameters)
                .withName("testSimpleInteractiveNotebookJobWithParams "+now)
                .create();

        Assert.assertEquals("Job CreatorUserName does not equal " + _databricksConfig.getWorkspaceUsername(),
                _databricksConfig.getWorkspaceUsername(), job.getCreatorUserName());

        Assert.assertEquals("Job Parameters is not 2", 2, job.getBaseParameters().size());

        //run job
        InteractiveNotebookJobRun jobRun = job.run();

        Assert.assertEquals("Job Run CreatorUserName does not equal " + _databricksConfig.getWorkspaceUsername(),
                _databricksConfig.getWorkspaceUsername(), jobRun.getCreatorUserName());

        Assert.assertEquals("Job Run Override is not zero", 0, jobRun.getOverridingParameters().size());

        Assert.assertEquals("Parameter 1 was not set", "Hello",
                jobRun.getBaseParameters().get("parameter1"));

        Assert.assertEquals("Parameter 2 was not set", "World",
                jobRun.getBaseParameters().get("parameter2"));

        while(!jobRun.getRunState().LifeCycleState.isFinal()) {
            Thread.sleep(5000); //wait 5 seconds
        }

        Assert.assertEquals("Job Output Does Not Match", "This is Parameter 1: Hello, and this is Parameter 2: World",
                jobRun.getOutput());

        HashMap<String,String> parameterOverride = new HashMap<String,String>();
        parameterOverride.put("parameter1", "Override One");
        parameterOverride.put("parameter2", "Override Two");

        InteractiveNotebookJobRun jobRunWithParamOverride = job.run(parameterOverride);

        Assert.assertEquals("Override Parameter 1 was not set", "Override One",
                jobRunWithParamOverride.getOverridingParameters().get("parameter1"));

        Assert.assertEquals("Override Parameter 2 was not set", "Override Two",
                jobRunWithParamOverride.getOverridingParameters().get("parameter2"));

        while(!jobRunWithParamOverride.getRunState().LifeCycleState.isFinal()) {
            Thread.sleep(5000); //wait 5 seconds
        }

        Assert.assertEquals("Job Output Does Not Match", "This is Parameter 1: Override One, and this is Parameter 2: Override Two",
                jobRunWithParamOverride.getOutput());

        //cleanup
        job.delete();
        cluster.terminate();
        scalaNotebook.delete();
    }



}
