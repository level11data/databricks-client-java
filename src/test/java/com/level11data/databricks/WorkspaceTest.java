package com.level11data.databricks;

import com.level11data.databricks.client.DatabricksSession;
import com.level11data.databricks.config.DatabricksClientConfiguration;
import com.level11data.databricks.util.TestUtils;
import com.level11data.databricks.workspace.*;
import com.level11data.databricks.workspace.util.WorkspaceHelper;
import org.junit.Assert;
import org.junit.Test;
import static org.hamcrest.CoreMatchers.instanceOf;
import java.io.File;

public class WorkspaceTest {
    public static final String SIMPLE_SCALA_DBC_NOTEBOOK_RESOURCE_NAME = "test-notebook.dbc";
    public static final String SIMPLE_SCALA_SOURCE_NOTEBOOK_RESOURCE_NAME = "test-notebook.scala";

    //load config from default resource databricks-client.properties (in test/resources)
    DatabricksClientConfiguration _databricksConfig = new DatabricksClientConfiguration();

    DatabricksSession _databricks = new DatabricksSession(_databricksConfig);

    public WorkspaceTest() throws Exception {

    }

    @Test
    public void testScalaDbcNotebook() throws Exception {
        long now = System.currentTimeMillis();

        File localFile = TestUtils.getResourceByName(SIMPLE_SCALA_DBC_NOTEBOOK_RESOURCE_NAME);

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

        String localFilename = System.getProperty("java.io.tmpdir") + uniqueName + ".scala";
        File downloadedFile = _databricks.getNotebook(workspaceNotebookPath).saveAsSource(localFilename);

        //TODO download source with client and compare to .saveAsSource()
        //_databricks.getWorkspaceClient().exportResource()



        //cleanup the test
        scalaNotebook.delete();
        _databricks.deleteWorkspaceObject(workspacePath, false);
        downloadedFile.delete();
    }

    @Test
    public void testScalaHtmlNotebook() throws Exception {
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

        String localFilename = System.getProperty("java.io.tmpdir") + uniqueName + ".html";
        File downloadedFile = _databricks.getNotebook(workspaceNotebookPath).saveAsHtml(localFilename);

        //cleanup the test
        scalaNotebook.delete();
        _databricks.deleteWorkspaceObject(workspacePath, false);
        downloadedFile.delete();
    }


}
