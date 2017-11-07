package com.level11data.databricks.client;

import com.level11data.databricks.config.DatabricksClientConfiguration;
import com.level11data.databricks.job.AutomatedNotebookJob;
import com.level11data.databricks.job.AutomatedNotebookJobRun;
import com.level11data.databricks.job.builder.AutomatedNotebookJobBuilder;
import com.level11data.databricks.workspace.Notebook;
import org.junit.Assert;
import org.junit.Test;

import java.io.InputStream;

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
        String clusterName = "test simple automated notebook job " + now;
        int numberOfExecutors = 1;

        //create job
        //TODO Implement Workspace API to import notebook from resources
        String notebookPath = "/Users/" + _databricksConfig.getClientUsername() + "/test-notebook";
        Notebook notebook = new Notebook(notebookPath);

        AutomatedNotebookJob job = _databricks.createJob(notebook)
                .withName("testSimpleAutomatedNotebookJob")
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


}
