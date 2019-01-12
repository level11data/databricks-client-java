package com.level11data.databricks;

import com.level11data.databricks.client.DatabricksSession;
import com.level11data.databricks.cluster.ClusterSpec;
import com.level11data.databricks.config.DatabricksClientConfiguration;
import com.level11data.databricks.job.AutomatedNotebookJob;
import com.level11data.databricks.job.run.AutomatedNotebookJobRun;
import com.level11data.databricks.job.run.RunResultState;
import com.level11data.databricks.util.TestUtils;
import com.level11data.databricks.workspace.ScalaNotebook;
import org.junit.Assert;
import org.junit.Test;

import java.io.File;
import java.util.HashMap;

public class AutomatedNotebookJobTest {
    //load config from default resource databricks-client.properties (in test/resources)
    DatabricksClientConfiguration _databricksConfig = new DatabricksClientConfiguration();

    DatabricksSession _databricks = new DatabricksSession(_databricksConfig);

    public static final String SIMPLE_SCALA_SOURCE_NOTEBOOK_RESOURCE_NAME = "test-notebook.scala";
    public static final String SIMPLE_SCALA_PARAMETERS_SOURCE_NOTEBOOK_RESOURCE_NAME = "test-notebook-parameters.scala";

    public AutomatedNotebookJobTest() throws Exception {

    }

    @Test
    public void testSimpleAutomatedNotebookJob() throws Exception {
        long now = System.currentTimeMillis();

        //Set job name to ClassName.MethodName TIMESTAMP
        String jobName = this.getClass().getSimpleName() + "." +
                Thread.currentThread().getStackTrace()[1].getMethodName() +
                "-" +now;

        //create cluster spec
        ClusterSpec clusterSpec = _databricks.createClusterSpec(1)
                .withSparkVersion("3.4.x-scala2.11")
                .withNodeType("i3.xlarge")
                .createClusterSpec();

        //create notebook
        File localFile = TestUtils.getResourceByName(SIMPLE_SCALA_SOURCE_NOTEBOOK_RESOURCE_NAME);
        String workspacePath = "/tmp/test/" + jobName;
        String workspaceNotebookPath = workspacePath + "/" + SIMPLE_SCALA_SOURCE_NOTEBOOK_RESOURCE_NAME;

        ScalaNotebook scalaNotebook = _databricks.createScalaNotebook(localFile).create(workspaceNotebookPath);

        //create job
        AutomatedNotebookJob job = _databricks.createJob(scalaNotebook)
                .withName(jobName)
                .withClusterSpec(clusterSpec)
                .create();

        Assert.assertEquals("Job CreatorUserName does not equal " + _databricksConfig.getClientUsername(),
                _databricksConfig.getClientUsername(), job.getCreatorUserName());

        Assert.assertEquals("Job Parameters is not zero", 0, job.BaseParameters.size());

        //run job
        AutomatedNotebookJobRun jobRun = job.run();

        Assert.assertEquals("Job Run CreatorUserName does not equal " + _databricksConfig.getClientUsername(),
                _databricksConfig.getClientUsername(), jobRun.CreatorUserName);

        Assert.assertEquals("Job Run Override is not zero", 0, jobRun.OverridingParameters.size());

        while(!jobRun.getRunState().LifeCycleState.isFinal()) {
            Thread.sleep(5000); //wait 5 seconds
        }

        Assert.assertEquals("Job Run Not Successful",
                RunResultState.SUCCESS,  jobRun.getRunState().ResultState);

        Assert.assertEquals("Job Run Output Does Not Match", "2", jobRun.getOutput());

        //cleanup
        job.delete();
        scalaNotebook.delete();
    }

    @Test
    public void testSimpleAutomatedNotebookJobWithParameters() throws Exception {
        long now = System.currentTimeMillis();

        //Set cluster name to ClassName.MethodName TIMESTAMP
        String jobName = this.getClass().getSimpleName() + "." +
                Thread.currentThread().getStackTrace()[1].getMethodName() +
                "-" +now;

        //create notebook
        File localFile = TestUtils.getResourceByName(SIMPLE_SCALA_PARAMETERS_SOURCE_NOTEBOOK_RESOURCE_NAME);
        String workspacePath = "/tmp/test/" + jobName;
        String workspaceNotebookPath = workspacePath + "/" + SIMPLE_SCALA_PARAMETERS_SOURCE_NOTEBOOK_RESOURCE_NAME;
        ScalaNotebook scalaNotebook = _databricks.createScalaNotebook(localFile).create(workspaceNotebookPath);

        HashMap<String,String> parameters = new HashMap<String,String>();
        parameters.put("parameter1", "Hello");
        parameters.put("parameter2", "World");

        //create cluster spec
        ClusterSpec clusterSpec = _databricks.createClusterSpec(1)
                .withSparkVersion("3.4.x-scala2.11")
                .withNodeType("i3.xlarge")
                .createClusterSpec();

        //create job
        AutomatedNotebookJob job = _databricks.createJob(scalaNotebook, parameters)
                .withName(jobName)
                .withClusterSpec(clusterSpec)
                .create();

        Assert.assertEquals("Job CreatorUserName does not equal " + _databricksConfig.getClientUsername(),
                _databricksConfig.getClientUsername(), job.getCreatorUserName());

        Assert.assertEquals("Job Parameters is not 2", 2, job.BaseParameters.size());

        //run job
        AutomatedNotebookJobRun jobRun = job.run();

        Assert.assertEquals("Job Run CreatorUserName does not equal " + _databricksConfig.getClientUsername(),
                _databricksConfig.getClientUsername(), jobRun.CreatorUserName);

        Assert.assertEquals("Job Run Override is not zero", 0, jobRun.OverridingParameters.size());

        Assert.assertEquals("Parameter 1 was not set", "Hello",
                jobRun.BaseParameters.get("parameter1"));

        Assert.assertEquals("Parameter 2 was not set", "World",
                jobRun.BaseParameters.get("parameter2"));

        while(!jobRun.getRunState().LifeCycleState.isFinal()) {
            Thread.sleep(5000); //wait 5 seconds
        }

        Assert.assertEquals("Job Run Not Successful",
                RunResultState.SUCCESS,  jobRun.getRunState().ResultState);

        Assert.assertEquals("Job Output Does Not Match", "This is Parameter 1: Hello, and this is Parameter 2: World",
                jobRun.getOutput());

        HashMap<String,String> parameterOverride = new HashMap<String,String>();
        parameterOverride.put("parameter1", "Override One");
        parameterOverride.put("parameter2", "Override Two");

        AutomatedNotebookJobRun jobRunWithParamOverride = job.run(parameterOverride);

        Assert.assertEquals("Override Parameter 1 was not set", "Override One",
                jobRunWithParamOverride.OverridingParameters.get("parameter1"));

        Assert.assertEquals("Override Parameter 2 was not set", "Override Two",
                jobRunWithParamOverride.OverridingParameters.get("parameter2"));

        while(!jobRunWithParamOverride.getRunState().LifeCycleState.isFinal()) {
            Thread.sleep(5000); //wait 5 seconds
        }

        Assert.assertEquals("Job Run Not Successful",
                RunResultState.SUCCESS,  jobRunWithParamOverride.getRunState().ResultState);

        Assert.assertEquals("Job Output Does Not Match", "This is Parameter 1: Override One, and this is Parameter 2: Override Two",
                jobRunWithParamOverride.getOutput());

        //cleanup
        job.delete();
        scalaNotebook.delete();
    }

}
