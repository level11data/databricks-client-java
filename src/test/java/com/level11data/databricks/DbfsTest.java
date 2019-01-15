package com.level11data.databricks;

import com.level11data.databricks.session.WorkspaceSession;
import com.level11data.databricks.config.DatabricksClientConfiguration;
import com.level11data.databricks.dbfs.DbfsFileInfo;
import org.junit.Assert;
import org.junit.Test;

import java.io.File;
import java.io.FileOutputStream;

public class DbfsTest {
    public static final String SIMPLE_JAR_RESOURCE_NAME = "simple-scala-spark-app_2.11-0.0.1.jar";

    //load config from default resource databricks-client.properties (in test/resources)
    DatabricksClientConfiguration _databricksConfig = new DatabricksClientConfiguration();

    WorkspaceSession _databricks = new WorkspaceSession(_databricksConfig);

    public DbfsTest() throws Exception {

    }

    @Test
    public void testDirectory() throws Exception {
        long now = System.currentTimeMillis();

        ClassLoader loader = Thread.currentThread().getContextClassLoader();

        //Set to ClassName.MethodName-TIMESTAMP
        String uniqueName = this.getClass().getSimpleName() + "." +
                Thread.currentThread().getStackTrace()[1].getMethodName() +
                "-" +now;

        String dbfsPath = "/tmp/test/"+uniqueName+"/test-dir";

        _databricks.mkdirsDbfs(dbfsPath);
        DbfsFileInfo fileInfo = _databricks.getDbfsObjectStatus(dbfsPath);

        Assert.assertEquals("Dbfs Object is not recognized as a directory",
                true, fileInfo.IsDir);

        //cleanup
        _databricks.deleteDbfsObject(dbfsPath, false);
    }

    @Test
    public void testSmallFile() throws Exception {
        long now = System.currentTimeMillis();

        ClassLoader loader = Thread.currentThread().getContextClassLoader();
        String localPath = loader.getResource(SIMPLE_JAR_RESOURCE_NAME).getFile(); //less than 1MB

        //Set to ClassName.MethodName-TIMESTAMP
        String uniqueName = this.getClass().getSimpleName() + "." +
                Thread.currentThread().getStackTrace()[1].getMethodName() +
                "-" +now;

        String dbfsPath = "/tmp/test/"+uniqueName+"/"+SIMPLE_JAR_RESOURCE_NAME;

        String tmpPath = "/tmp/"+now+"-"+SIMPLE_JAR_RESOURCE_NAME;

        File file = new File(localPath);
        long srcFileSize = file.length();

        _databricks.putDbfsFile(file, dbfsPath, true);

        DbfsFileInfo fileStatus = _databricks.getDbfsObjectStatus(dbfsPath);

        Assert.assertEquals("Object on DBFS is incorrectly classified as a directory",
                false, fileStatus.IsDir);

        byte[] downloadedBytes = _databricks.getDbfsObject(dbfsPath);

        FileOutputStream fos = new FileOutputStream(tmpPath);
        fos.write(downloadedBytes);
        fos.close();

        long downloadedFileSize = new File(tmpPath).length();

        Assert.assertEquals("Pre-uploaded file size is different from file size after download",
                srcFileSize, downloadedFileSize);

        //cleanup test files
        _databricks.deleteDbfsObject(dbfsPath, false);
        new File(tmpPath).delete();
    }

    @Test
    public void testLargeFile() throws Exception {
        long now = System.currentTimeMillis();

        ClassLoader loader = Thread.currentThread().getContextClassLoader();
        String localPath = loader.getResource("large-file.zip").getFile(); //greater than 1MB

        //Set to ClassName.MethodName-TIMESTAMP
        String uniqueName = this.getClass().getSimpleName() + "." +
                Thread.currentThread().getStackTrace()[1].getMethodName() +
                "-" +now;

        String dbfsPath = "/tmp/test/"+uniqueName+"/large-file.zip";
        String tmpPath = "/tmp/"+now+"-large-file.zip";

        File file = new File(localPath);
        long srcFileSize = file.length();

        _databricks.putDbfsFile(file, dbfsPath, true);

        DbfsFileInfo fileStatus = _databricks.getDbfsObjectStatus(dbfsPath);

        Assert.assertEquals("Object on DBFS is incorrectly classified as a directory",
                false, fileStatus.IsDir);

        byte[] downloadedBytes = _databricks.getDbfsObject(dbfsPath);

        FileOutputStream fos = new FileOutputStream(tmpPath);
        fos.write(downloadedBytes);
        fos.close();

        long downloadedFileSize = new File(tmpPath).length();

        Assert.assertEquals("Downloaded file size is different from file size on DBFS",
                _databricks.getDbfsObjectStatus(dbfsPath).FileSize, downloadedFileSize);

        //cleanup test files
        _databricks.deleteDbfsObject(dbfsPath, false);
        new File(tmpPath).delete();
    }

}