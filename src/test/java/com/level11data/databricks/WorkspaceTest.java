package com.level11data.databricks;

import com.level11data.databricks.client.DatabricksSession;
import com.level11data.databricks.config.DatabricksClientConfiguration;
import com.level11data.databricks.workspace.*;
import com.level11data.databricks.workspace.util.WorkspaceHelper;
import org.junit.Assert;
import org.junit.Test;
import static org.hamcrest.CoreMatchers.instanceOf;
import java.io.File;
import java.io.InputStream;

public class WorkspaceTest {

    public static final String CLIENT_CONFIG_RESOURCE_NAME = "test.properties";
    public static final String SIMPLE_SCALA_DBC_NOTEBOOK_RESOURCE_NAME = "test-notebook.dbc";
    public static final String SIMPLE_SCALA_SOURCE_NOTEBOOK_RESOURCE_NAME = "test-notebook.scala";

    ClassLoader loader = Thread.currentThread().getContextClassLoader();
    InputStream resourceStream = loader.getResourceAsStream(CLIENT_CONFIG_RESOURCE_NAME);
    DatabricksSession _databricks;
    DatabricksClientConfiguration _databricksConfig;

    public WorkspaceTest() throws Exception {
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
    public void testScalaDbcNotebook() throws Exception {
        long now = System.currentTimeMillis();

        ClassLoader loader = Thread.currentThread().getContextClassLoader();
        String localPath = loader.getResource(SIMPLE_SCALA_DBC_NOTEBOOK_RESOURCE_NAME).getFile();
        File localFile = new File(localPath);

        //Set to ClassName.MethodName-TIMESTAMP
        String uniqueName = this.getClass().getSimpleName() + "." +
                Thread.currentThread().getStackTrace()[1].getMethodName() +
                "-" + now;

        String workspacePath = "/tmp/test/" + uniqueName;
        String workspaceNotebookPath = workspacePath + "/" + SIMPLE_SCALA_DBC_NOTEBOOK_RESOURCE_NAME;

        ScalaNotebook scalaNotebook = _databricks.createScalaNotebook(localFile).create(workspaceNotebookPath);

        WorkspaceHelper helper = new WorkspaceHelper(_databricks.getWorkspaceClient());

        ObjectStatus notebookStatus = helper.getStatus(workspaceNotebookPath);

        Assert.assertEquals("Imported ObjectType is not Notebook", ObjectType.NOTEBOOK, notebookStatus.Type);
        Assert.assertEquals("Imported Notebook base language is not Scala", NotebookLanguage.SCALA, notebookStatus.NotebookLanguage);

        Notebook notebook = _databricks.getNotebook(workspaceNotebookPath);
        Assert.assertThat("Notebook Interface Check", notebook, instanceOf(ScalaNotebook.class));

        String localFilename = System.getProperty("java.io.tmpdir") + uniqueName + ".dbc";
        File downloadedFile = _databricks.getNotebook(workspaceNotebookPath).saveAsDbc(localFilename);

        //TODO is it right to expect the hashes to match after upload & download?
        //System.out.println(downloadedFile.toString());
        //Assert.assertEquals("Downloaded dbc file does not match pre-upload file",
        //        ResourceUtils.getMD5(localFile), ResourceUtils.getMD5(downloadedFile));

        //cleanup the test
        scalaNotebook.delete();
        _databricks.deleteWorkspaceObject(workspacePath, false);
        downloadedFile.delete();
    }

    @Test
    public void testScalaSourceNotebook() throws Exception {
        long now = System.currentTimeMillis();

        ClassLoader loader = Thread.currentThread().getContextClassLoader();
        String localPath = loader.getResource(SIMPLE_SCALA_SOURCE_NOTEBOOK_RESOURCE_NAME).getFile();
        File localFile = new File(localPath);

        //Set to ClassName.MethodName-TIMESTAMP
        String uniqueName = this.getClass().getSimpleName() + "." +
                Thread.currentThread().getStackTrace()[1].getMethodName() +
                "-" + now;

        String workspacePath = "/tmp/test/" + uniqueName;
        String workspaceNotebookPath = workspacePath + "/" + SIMPLE_SCALA_SOURCE_NOTEBOOK_RESOURCE_NAME;


        ScalaNotebook scalaNotebook = _databricks.createScalaNotebook(localFile).create(workspaceNotebookPath);

        WorkspaceHelper helper = new WorkspaceHelper(_databricks.getWorkspaceClient());

        ObjectStatus notebookStatus = helper.getStatus(workspaceNotebookPath);

        Assert.assertEquals("Imported ObjectType is not Notebook", ObjectType.NOTEBOOK, notebookStatus.Type);
        Assert.assertEquals("Imported Notebook base language is not Scala", NotebookLanguage.SCALA, notebookStatus.NotebookLanguage);

        Notebook notebook = _databricks.getNotebook(workspaceNotebookPath);
        Assert.assertThat("Notebook Interface Check", notebook, instanceOf(ScalaNotebook.class));

        String localFilename = System.getProperty("java.io.tmpdir") + uniqueName + ".dbc";
        File downloadedFile = _databricks.getNotebook(workspaceNotebookPath).saveAsDbc(localFilename);

        //TODO is it right to expect the hashes to match after upload & download?
        //System.out.println(downloadedFile.toString());
        //Assert.assertEquals("Downloaded source file does not match pre-upload file",
        //        ResourceUtils.getMD5(localFile), ResourceUtils.getMD5(downloadedFile));

        //cleanup the test
        scalaNotebook.delete();
        _databricks.deleteWorkspaceObject(workspacePath, false);
        downloadedFile.delete();
    }


}
