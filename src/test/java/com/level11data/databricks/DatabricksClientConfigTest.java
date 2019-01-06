package com.level11data.databricks;

import com.level11data.databricks.config.DatabricksClientConfigException;
import com.level11data.databricks.config.DatabricksClientConfiguration;
import org.junit.Assert;
import org.apache.commons.io.FileUtils;
import org.junit.Test;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

public class DatabricksClientConfigTest {

    private File _validConfigFile;

    String TEST_URL_VALUE = "https://www.databricks.com";
    String TEST_TOKEN_VALUE = "FAKE_TEST_TOKEN";
    String TEST_USER_VALUE = "FAKE_TEST_USER";
    String TEST_PASS_VALUE = "FAKE_TEST_PASS";

    public DatabricksClientConfigTest() throws Exception {

    }

    private File getValidConfigFile() throws IOException {
        if(_validConfigFile == null) {
            File tmpFile = File.createTempFile("databricks-client.test.config-valid",".tmp");
            tmpFile.deleteOnExit();

            String COMMENT_LINE = "#connection details to Databricks";
            String URL_LINE = "com.level11data.databricks.client.url = " + TEST_URL_VALUE;
            String TOKEN_LINE = "com.level11data.databricks.client.token = " + TEST_TOKEN_VALUE;
            String USERNAME_LINE = "com.level11data.databricks.client.username = " + TEST_USER_VALUE;
            String PASSWORD_LINE = "com.level11data.databricks.client.password = " + TEST_PASS_VALUE;

            List<String> lines = Arrays.asList(COMMENT_LINE, URL_LINE, TOKEN_LINE, USERNAME_LINE, PASSWORD_LINE);

            FileUtils.writeLines(tmpFile, lines);

            _validConfigFile = tmpFile;
        }
        return _validConfigFile;
    }

    private File getInValidConfigFileNoUrl() throws IOException {
        File tmpFile = File.createTempFile("databricks-client.test.config-nourl",".tmp");
        tmpFile.deleteOnExit();

        String COMMENT_LINE = "#connection details to Databricks";
        String TOKEN_LINE = "com.level11data.databricks.client.token = " + TEST_TOKEN_VALUE;
        String USERNAME_LINE = "com.level11data.databricks.client.username = " + TEST_USER_VALUE;
        String PASSWORD_LINE = "com.level11data.databricks.client.password = " + TEST_PASS_VALUE;

        List<String> lines = Arrays.asList(COMMENT_LINE, TOKEN_LINE, USERNAME_LINE, PASSWORD_LINE);

        FileUtils.writeLines(tmpFile, lines);

        return tmpFile;
    }

    private File getInValidConfigFileNoTokenOrUser() throws IOException {
        File tmpFile = File.createTempFile("databricks-client.test.config-notokenoruser",".tmp");
        tmpFile.deleteOnExit();

        String COMMENT_LINE = "#connection details to Databricks";
        String URL_LINE = "com.level11data.databricks.client.url = " + TEST_URL_VALUE;

        List<String> lines = Arrays.asList(COMMENT_LINE, URL_LINE);

        FileUtils.writeLines(tmpFile, lines);

        return tmpFile;
    }

    private File getInValidConfigFileNoPass() throws IOException {
        File tmpFile = File.createTempFile("databricks-client.test.config-notokenoruser",".tmp");
        tmpFile.deleteOnExit();

        String COMMENT_LINE = "#connection details to Databricks";
        String URL_LINE = "com.level11data.databricks.client.url = " + TEST_URL_VALUE;
        String USERNAME_LINE = "com.level11data.databricks.client.username = " + TEST_USER_VALUE;

        List<String> lines = Arrays.asList(COMMENT_LINE, URL_LINE, USERNAME_LINE);

        FileUtils.writeLines(tmpFile, lines);

        return tmpFile;
    }

    @Test
    public void testLoadConfigFromFile() throws Exception {
        File configFile = getValidConfigFile();

        DatabricksClientConfiguration databricksClientConfig = new DatabricksClientConfiguration(configFile);

        Assert.assertEquals("URL value in Config is different than expected",
                TEST_URL_VALUE, databricksClientConfig.getClientUrl().toString());

        Assert.assertEquals("Token value in Config is different than expected",
                TEST_TOKEN_VALUE, databricksClientConfig.getClientToken().toString());

        Assert.assertEquals("Username value in Config is different than expected",
                TEST_USER_VALUE, databricksClientConfig.getClientUsername().toString());

        Assert.assertEquals("Password value in Config is different than expected",
                TEST_PASS_VALUE, databricksClientConfig.getClientPassword().toString());
    }


    @Test
    public void testLoadConfigFromInputStream() throws Exception {
        File configFile = getValidConfigFile();
        InputStream inputStream = new FileInputStream(configFile);

        DatabricksClientConfiguration databricksClientConfig = new DatabricksClientConfiguration(inputStream);

        Assert.assertEquals("URL value in Config is different than expected",
                TEST_URL_VALUE, databricksClientConfig.getClientUrl().toString());

        Assert.assertEquals("Token value in Config is different than expected",
                TEST_TOKEN_VALUE, databricksClientConfig.getClientToken().toString());

        Assert.assertEquals("Username value in Config is different than expected",
                TEST_USER_VALUE, databricksClientConfig.getClientUsername().toString());

        Assert.assertEquals("Password value in Config is different than expected",
                TEST_PASS_VALUE, databricksClientConfig.getClientPassword().toString());
    }

    @Test
    public void testInvalidConfigNoUrl() throws Exception {
        File configFile = getInValidConfigFileNoUrl();

        try{
            DatabricksClientConfiguration databricksClientConfig = new DatabricksClientConfiguration(configFile);
            throw new DatabricksClientConfigException("Client Config Should Have Failed to Initialize");
        }catch(DatabricksClientConfigException e) {
            Assert.assertEquals("Exception message does not match expectation",
                    "Databricks Client Config missing com.level11data.databricks.client.url", e.getMessage());
        }
    }

    @Test
         public void testInvalidConfigNoTokenOrUser() throws Exception {
        File configFile = getInValidConfigFileNoTokenOrUser();

        try{
            DatabricksClientConfiguration databricksClientConfig = new DatabricksClientConfiguration(configFile);
            throw new DatabricksClientConfigException("Client Config Should Have Failed to Initialize");
        }catch(DatabricksClientConfigException e) {
            Assert.assertEquals("Exception message does not match expectation",
                    "Databricks Client Config missing either com.level11data.databricks.client.token or com.level11data.databricks.client.username", e.getMessage());
        }
    }

    @Test
    public void testInvalidConfigNoPass() throws Exception {
        File configFile = getInValidConfigFileNoPass();

        try{
            DatabricksClientConfiguration databricksClientConfig = new DatabricksClientConfiguration(configFile);
            throw new DatabricksClientConfigException("Client Config Should Have Failed to Initialize");
        }catch(DatabricksClientConfigException e) {
            Assert.assertEquals("Exception message does not match expectation",
                    "Databricks Client Config missing com.level11data.databricks.client.password", e.getMessage());
        }
    }

    @Test
    public void testConfigFromDefaultResource() throws Exception {
        DatabricksClientConfiguration config = new DatabricksClientConfiguration();

        Assert.assertEquals("test.parameter from default resource does not match expected value",
                "test.value", config.getString("test.parameter"));
    }

    @Test
    public void testFromTokenConstructor() throws Exception {
        URI databricksURL = new URI("https://www.databricks.com");
        DatabricksClientConfiguration config = new DatabricksClientConfiguration(databricksURL,"fake-token");

        Assert.assertEquals("Token constructor token does not match",
                "fake-token", config.getClientToken());

        Assert.assertEquals("Token constructor URL does not match",
                databricksURL, config.getClientUrl());
    }
}
