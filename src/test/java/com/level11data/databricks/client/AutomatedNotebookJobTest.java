package com.level11data.databricks.client;

import com.level11data.databricks.config.DatabricksClientConfiguration;
import com.level11data.databricks.job.AutomatedNotebookJob;
import com.level11data.databricks.job.AutomatedNotebookJobRun;
import com.level11data.databricks.job.builder.AutomatedNotebookJobBuilder;
import com.level11data.databricks.workspace.Notebook;
import org.junit.Assert;
import org.junit.Test;

import java.io.InputStream;
import java.util.HashMap;

public class AutomatedNotebookJobTest {

    public static final String CLIENT_CONFIG_RESOURCE_NAME = "test.properties";

    ClassLoader loader = Thread.currentThread().getContextClassLoader();
    InputStream resourceStream = loader.getResourceAsStream(CLIENT_CONFIG_RESOURCE_NAME);
    DatabricksSession _databricks;
    DatabricksClientConfiguration _databricksConfig;

    public AutomatedNotebookJobTest() throws Exception {
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
    public void testSimpleAutomatedNotebookJob() throws Exception {
        long now = System.currentTimeMillis();
        int numberOfExecutors = 1;

        //create job
        //TODO Implement Workspace API to import notebook from resources
        String notebookPath = "/Users/" + _databricksConfig.getClientUsername() + "/test-notebook";
        Notebook notebook = new Notebook(notebookPath);

        AutomatedNotebookJob job = _databricks.createJob(notebook)
                .withName("testSimpleAutomatedNotebookJob " + now)
                .withClusterSpec(1)
                .addToJob(AutomatedNotebookJobBuilder.class)
                .create();

        Assert.assertEquals("Job CreatorUserName does not equal " + _databricksConfig.getClientUsername(),
                _databricksConfig.getClientUsername(), job.getCreatorUserName());

        Assert.assertEquals("Job Parameters is not zero", 0, job.BaseParameters.size());

        //run job
        AutomatedNotebookJobRun jobRun = job.run();

        Assert.assertEquals("Job Run CreatorUserName does not equal " + _databricksConfig.getClientUsername(),
                _databricksConfig.getClientUsername(), jobRun.CreatorUserName);

        Assert.assertEquals("Job Run Override is not zero", 0, jobRun.OverridingParameters.size());

        //cleanup
        job.delete();
    }

    @Test
    public void testSimpleAutomatedNotebookJobWithParameters() throws Exception {
        long now = System.currentTimeMillis();
        int numberOfExecutors = 1;

        //create job
        //TODO Implement Workspace API to import notebook from resources
        String notebookPath = "/Users/" + _databricksConfig.getClientUsername() + "/test-notebook-parameters";
        Notebook notebook = new Notebook(notebookPath);

        HashMap<String,String> parameters = new HashMap<String,String>();
        parameters.put("parameter1", "Hello");
        parameters.put("parameter2", "World");

        AutomatedNotebookJob job = _databricks.createJob(notebook, parameters)
                .withName("testSimpleAutomatedNotebookJobWithParameters "+now)
                .withClusterSpec(1)
                .addToJob(AutomatedNotebookJobBuilder.class)
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

        HashMap<String,String> parameterOverride = new HashMap<String,String>();
        parameterOverride.put("parameter1", "Override One");
        parameterOverride.put("parameter2", "Override Two");

        AutomatedNotebookJobRun jobRunWithParamOverride = job.run(parameterOverride);

        Assert.assertEquals("Override Parameter 1 was not set", "Override One",
                jobRunWithParamOverride.OverridingParameters.get("parameter1"));

        Assert.assertEquals("Override Parameter 2 was not set", "Override Two",
                jobRunWithParamOverride.OverridingParameters.get("parameter2"));

        //cleanup
        job.delete();
    }

}
