package com.level11data.databricks;

import com.level11data.databricks.client.DatabricksSession;
import com.level11data.databricks.cluster.ClusterState;
import com.level11data.databricks.cluster.InteractiveCluster;
import com.level11data.databricks.config.DatabricksClientConfiguration;
import com.level11data.databricks.job.InteractiveNotebookJob;
import com.level11data.databricks.job.run.InteractiveNotebookJobRun;
import com.level11data.databricks.workspace.Notebook;
import org.junit.Assert;
import org.junit.Test;

import java.io.InputStream;
import java.util.HashMap;

public class InteractiveNotebookJobTest {
    public static final String CLIENT_CONFIG_RESOURCE_NAME = "test.properties";

    ClassLoader loader = Thread.currentThread().getContextClassLoader();
    InputStream resourceStream = loader.getResourceAsStream(CLIENT_CONFIG_RESOURCE_NAME);
    DatabricksSession _databricks;
    DatabricksClientConfiguration _databricksConfig;

    public InteractiveNotebookJobTest() throws Exception {
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
    public void testSimpleInteractiveNotebookJob() throws Exception {
        long now = System.currentTimeMillis();

        //Set cluster name to ClassName.MethodName TIMESTAMP
        String clusterName = this.getClass().getSimpleName() + "." +
                Thread.currentThread().getStackTrace()[1].getMethodName() +
                " " +now;

        //Create Interactive AbstractCluster
        InteractiveCluster cluster = _databricks.createInteractiveCluster(clusterName, 1)
                .withAutoTerminationMinutes(20)
                .withSparkVersion("3.4.x-scala2.11")
                .withNodeType("i3.xlarge")
                .create();

        while(cluster.getState() == ClusterState.PENDING) {
            //wait until cluster is properly started
            // should not take more than 100 seconds from a cold start
            Thread.sleep(10000); //wait 10 seconds
        }

        //create job
        //TODO Implement Workspace API to import notebook from resources
        String notebookPath = "/Users/" + _databricksConfig.getClientUsername() + "/test-notebook";
        Notebook notebook = new Notebook(notebookPath);

        InteractiveNotebookJob job = cluster.createJob(notebook)
                .withName("testSimpleInteractiveNotebookJob "+now)
                .create();

        Assert.assertEquals("AbstractJob CreatorUserName does not equal " + _databricksConfig.getClientUsername(),
                _databricksConfig.getClientUsername(), job.getCreatorUserName());

        Assert.assertEquals("AbstractJob Parameters is not zero", 0, job.BaseParameters.size());

        //run job
        InteractiveNotebookJobRun jobRun = job.run();

        Assert.assertEquals("AbstractJob Run CreatorUserName does not equal " + _databricksConfig.getClientUsername(),
                _databricksConfig.getClientUsername(), jobRun.CreatorUserName);

        Assert.assertEquals("AbstractJob Run Override is not zero", 0, jobRun.OverridingParameters.size());

        while(!jobRun.getRunState().LifeCycleState.isFinal()) {
            Thread.sleep(5000); //wait 5 seconds
        }

        Assert.assertEquals("AbstractJob Run Output Does Not Match", "2", jobRun.getOutput());

        //cleanup
        job.delete();
        cluster.terminate();
    }

    @Test
    public void testSimpleInteractiveNotebookJobWithParams() throws Exception {
        long now = System.currentTimeMillis();


        //Set cluster name to ClassName.MethodName TIMESTAMP
        String clusterName = this.getClass().getSimpleName() + "." +
                Thread.currentThread().getStackTrace()[1].getMethodName() +
                " " +now;

        //Create Interactive AbstractCluster
        InteractiveCluster cluster = _databricks.createInteractiveCluster(clusterName, 1)
                .withAutoTerminationMinutes(20)
                .withSparkVersion("3.4.x-scala2.11")
                .withNodeType("i3.xlarge")
                .create();

        while(cluster.getState() == ClusterState.PENDING) {
            //wait until cluster is properly started
            // should not take more than 100 seconds from a cold start
            Thread.sleep(10000); //wait 10 seconds
        }

        //create job
        //TODO Implement Workspace API to import notebook from resources
        String notebookPath = "/Users/" + _databricksConfig.getClientUsername() + "/test-notebook-parameters";
        Notebook notebook = new Notebook(notebookPath);

        HashMap<String,String> parameters = new HashMap<String,String>();
        parameters.put("parameter1", "Hello");
        parameters.put("parameter2", "World");

        InteractiveNotebookJob job = cluster.createJob(notebook, parameters)
                .withName("testSimpleInteractiveNotebookJobWithParams "+now)
                .create();

        Assert.assertEquals("AbstractJob CreatorUserName does not equal " + _databricksConfig.getClientUsername(),
                _databricksConfig.getClientUsername(), job.getCreatorUserName());

        Assert.assertEquals("AbstractJob Parameters is not 2", 2, job.BaseParameters.size());

        //run job
        InteractiveNotebookJobRun jobRun = job.run();

        Assert.assertEquals("AbstractJob Run CreatorUserName does not equal " + _databricksConfig.getClientUsername(),
                _databricksConfig.getClientUsername(), jobRun.CreatorUserName);

        Assert.assertEquals("AbstractJob Run Override is not zero", 0, jobRun.OverridingParameters.size());

        Assert.assertEquals("Parameter 1 was not set", "Hello",
                jobRun.BaseParameters.get("parameter1"));

        Assert.assertEquals("Parameter 2 was not set", "World",
                jobRun.BaseParameters.get("parameter2"));

        while(!jobRun.getRunState().LifeCycleState.isFinal()) {
            Thread.sleep(5000); //wait 5 seconds
        }

        Assert.assertEquals("AbstractJob Output Does Not Match", "This is Parameter 1: Hello, and this is Parameter 2: World",
                jobRun.getOutput());

        HashMap<String,String> parameterOverride = new HashMap<String,String>();
        parameterOverride.put("parameter1", "Override One");
        parameterOverride.put("parameter2", "Override Two");

        InteractiveNotebookJobRun jobRunWithParamOverride = job.run(parameterOverride);

        Assert.assertEquals("Override Parameter 1 was not set", "Override One",
                jobRunWithParamOverride.OverridingParameters.get("parameter1"));

        Assert.assertEquals("Override Parameter 2 was not set", "Override Two",
                jobRunWithParamOverride.OverridingParameters.get("parameter2"));

        while(!jobRunWithParamOverride.getRunState().LifeCycleState.isFinal()) {
            Thread.sleep(5000); //wait 5 seconds
        }

        Assert.assertEquals("AbstractJob Output Does Not Match", "This is Parameter 1: Override One, and this is Parameter 2: Override Two",
                jobRunWithParamOverride.getOutput());

        //cleanup
        job.delete();
        cluster.terminate();
    }



}
