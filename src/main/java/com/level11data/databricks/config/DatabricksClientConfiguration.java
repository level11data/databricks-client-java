package com.level11data.databricks.config;

import org.apache.commons.configuration2.PropertiesConfiguration;
import org.apache.commons.configuration2.ex.ConfigurationException;
import com.google.common.base.Preconditions;

import java.io.*;
import java.net.URI;
import javax.ws.rs.core.UriBuilder;
public class DatabricksClientConfiguration extends PropertiesConfiguration {
    public static final String DEFAULT_RESOURCE_NAME = "databricks-client.properties";
    public static final String LEVEL11DATA_PREFIX = "com.level11data";
    private static final String CLIENT_PREFIX = "com.level11data.databricks.client";
    private static final String WORKSPACE_PREFIX = "com.level11data.databricks.client.workspace";
    public static final String WORKSPACE_URL = "com.level11data.databricks.client.workspace.url";
    public static final String WORKSPACE_TOKEN = "com.level11data.databricks.client.workspace.token";
    public static final String WORKSPACE_USERNAME = "com.level11data.databricks.client.workspace.username";
    public static final String WORKSPACE_PASSWORD = "com.level11data.databricks.client.workspace.password";
    public String userAgent = "infoworks.io";

    public DatabricksClientConfiguration() throws DatabricksClientConfigException {
        this.initConfigFromDefaultResource();
        this.validateRequiredClientProps();
    }

    public DatabricksClientConfiguration(URI databricksURL, String token) throws DatabricksClientConfigException {
        this.addProperty("com.level11data.databricks.client.workspace.url", databricksURL.toString());
        this.addProperty("com.level11data.databricks.client.workspace.token", token);
        this.validateRequiredClientProps();
    }

    public DatabricksClientConfiguration(File propertiesFile) throws DatabricksClientConfigException {
        FileInputStream inputStream;
        try {
            inputStream = new FileInputStream(propertiesFile);
        } catch (FileNotFoundException var5) {
            throw new DatabricksClientConfigException("File Not Found: " + propertiesFile.getAbsolutePath(), var5);
        }

        this.readConfigFromStream(inputStream);

        try {
            inputStream.close();
        } catch (IOException var4) {
            throw new DatabricksClientConfigException(var4);
        }

        this.validateRequiredClientProps();
    }

    public DatabricksClientConfiguration(InputStream inputStream) throws DatabricksClientConfigException {
        this.readConfigFromStream(inputStream);
        this.validateRequiredClientProps();
    }

    private void initConfigFromDefaultResource() throws DatabricksClientConfigException {
        ClassLoader loader = Thread.currentThread().getContextClassLoader();
        InputStream resourceStream = loader.getResourceAsStream("databricks-client.properties");
        if (resourceStream == null) {
            throw new DatabricksClientConfigException("Default Resource Not Found: databricks-client.properties");
        } else {
            this.readConfigFromStream(resourceStream);

            try {
                resourceStream.close();
            } catch (IOException var4) {
                throw new DatabricksClientConfigException(var4);
            }
        }
    }

    private void readConfigFromStream(InputStream inputStream) throws DatabricksClientConfigException {
        try {
            this.read(new InputStreamReader(inputStream));
        } catch (IOException var3) {
            throw new DatabricksClientConfigException(var3);
        } catch (ConfigurationException var4) {
            throw new DatabricksClientConfigException(var4);
        }
    }

    public URI getWorkspaceUrl() {
        return UriBuilder.fromUri(this.getNonEmptyString("com.level11data.databricks.client.workspace.url")).build(new Object[0]);
    }

    public String getWorkspaceUsername() {
        return this.getNonEmptyString("com.level11data.databricks.client.workspace.username");
    }

    public String getWorkspacePassword() {
        return this.getNonEmptyString("com.level11data.databricks.client.workspace.password");
    }

    public String getWorkspaceToken() {
        return this.getNonEmptyString("com.level11data.databricks.client.workspace.token");
    }

    public boolean hasClientToken() {
        try {
            this.getWorkspaceToken();
            return true;
        } catch (NullPointerException var2) {
            return false;
        } catch (IllegalArgumentException var3) {
            return false;
        }
    }

    public boolean hasClientUsername() {
        try {
            this.getWorkspaceUsername();
            return true;
        } catch (NullPointerException var2) {
            return false;
        } catch (IllegalArgumentException var3) {
            return false;
        }
    }

    public boolean hasClientPassword() {
        try {
            this.getWorkspacePassword();
            return true;
        } catch (NullPointerException var2) {
            return false;
        } catch (IllegalArgumentException var3) {
            return false;
        }
    }

    public void setUserAgent(String userAgentVal) {
        this.userAgent = userAgentVal;
    }

    public String getUserAgent() {
        return this.userAgent;
    }

    private void validateRequiredClientProps() throws DatabricksClientConfigException {
        boolean valid = true;
        boolean var10000 = valid & this.verifyStringPropSet("com.level11data.databricks.client.workspace.url");
        if (!this.verifyStringPropSet("com.level11data.databricks.client.workspace.url")) {
            throw new DatabricksClientConfigException("Databricks Client Config missing com.level11data.databricks.client.workspace.url");
        } else if (!this.verifyStringPropSet("com.level11data.databricks.client.workspace.token") && !this.verifyStringPropSet("com.level11data.databricks.client.workspace.username")) {
            throw new DatabricksClientConfigException("Databricks Client Config missing either com.level11data.databricks.client.workspace.token or com.level11data.databricks.client.workspace.username");
        } else if (!this.verifyStringPropSet("com.level11data.databricks.client.workspace.token") && this.verifyStringPropSet("com.level11data.databricks.client.workspace.username") && !this.verifyStringPropSet("com.level11data.databricks.client.workspace.password")) {
            throw new DatabricksClientConfigException("Databricks Client Config missing com.level11data.databricks.client.workspace.password");
        }
    }

    private String getNonEmptyString(String property, String defaultValue) {
        String value = this.getString(property, defaultValue);
        Preconditions.checkNotNull(value, property + " cannot be null");
        Preconditions.checkArgument(!value.isEmpty(), property + " cannot be empty");
        return value;
    }

    private String getNonEmptyString(String property) {
        String value = this.getString(property);
        Preconditions.checkNotNull(value, property + " cannot be null");
        Preconditions.checkArgument(!value.isEmpty(), property + " cannot be empty");
        return value;
    }

    private static String verifyNotNull(String property, String value) {
        Preconditions.checkNotNull(value, property + " cannot be null");
        return value;
    }

    private boolean verifyStringPropSet(String key) {
        return this.containsKey(key) && !this.getString(key).isEmpty();
    }

    private boolean verifyStringPropNotSet(String key) {
        return !this.containsKey(key) || this.getString(key).isEmpty();
    }
}
