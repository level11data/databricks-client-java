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
    private static final String CLIENT_PREFIX = LEVEL11DATA_PREFIX + ".databricks.client";
    private static final String WORKSPACE_PREFIX = CLIENT_PREFIX + ".workspace";
    public static final String WORKSPACE_URL = WORKSPACE_PREFIX + ".url";
    public static final String WORKSPACE_TOKEN = WORKSPACE_PREFIX + ".token";
    public static final String WORKSPACE_USERNAME = WORKSPACE_PREFIX + ".username";
    public static final String WORKSPACE_PASSWORD = WORKSPACE_PREFIX + ".password";
    public String userAgent = "infoworks.io";

    public DatabricksClientConfiguration() throws DatabricksClientConfigException {
        super();
        initConfigFromDefaultResource();
        validateRequiredClientProps();
    }

    public DatabricksClientConfiguration(URI databricksURL, String token) throws DatabricksClientConfigException {
        super();
        this.addProperty(WORKSPACE_URL, databricksURL.toString());
        this.addProperty(WORKSPACE_TOKEN, token);
        validateRequiredClientProps();
    }

    public DatabricksClientConfiguration(File propertiesFile) throws DatabricksClientConfigException {
        super();
        FileInputStream inputStream;
        try {
            inputStream = new FileInputStream(propertiesFile);
        } catch (FileNotFoundException var5) {
            throw new DatabricksClientConfigException("File Not Found: " + propertiesFile.getAbsolutePath(), var5);
        }

        readConfigFromStream(inputStream);

        try {
            inputStream.close();
        } catch (IOException var4) {
            throw new DatabricksClientConfigException(var4);
        }

        validateRequiredClientProps();
    }

    public DatabricksClientConfiguration(InputStream inputStream) throws DatabricksClientConfigException {
        super();
        readConfigFromStream(inputStream);
        validateRequiredClientProps();
    }

    private void initConfigFromDefaultResource() throws DatabricksClientConfigException {
        ClassLoader loader = Thread.currentThread().getContextClassLoader();
        InputStream resourceStream = loader.getResourceAsStream(DEFAULT_RESOURCE_NAME);
        if (resourceStream == null) {
            throw new DatabricksClientConfigException("Default Resource Not Found: "+ DEFAULT_RESOURCE_NAME);
        }
        readConfigFromStream(resourceStream);
        try {
            resourceStream.close();
        } catch (IOException var4) {
            throw new DatabricksClientConfigException(var4);
        }
    }

    private void readConfigFromStream(InputStream inputStream) throws DatabricksClientConfigException {
        try {
            this.read(new InputStreamReader(inputStream));
        } catch (IOException e) {
            throw new DatabricksClientConfigException(e);
        } catch (ConfigurationException e) {
            throw new DatabricksClientConfigException(e);
        }
    }

    public URI getWorkspaceUrl() {
        return UriBuilder.fromUri(getNonEmptyString(WORKSPACE_URL)).build(new Object[0]);
    }

    public String getWorkspaceUsername() {
        return this.getNonEmptyString(WORKSPACE_USERNAME);
    }

    public String getWorkspacePassword() {
        return this.getNonEmptyString(WORKSPACE_PASSWORD);
    }

    public String getWorkspaceToken() {
        return this.getNonEmptyString(WORKSPACE_TOKEN);
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
            getWorkspaceUsername();
            return true;
        } catch (NullPointerException e) {
            return false;
        } catch (IllegalArgumentException e) {
            return false;
        }
    }

    public boolean hasClientPassword() {
        try {
            getWorkspacePassword();
            return true;
        } catch (NullPointerException e) {
            return false;
        } catch (IllegalArgumentException e) {
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
        valid &= verifyStringPropSet(WORKSPACE_URL);
        if (!this.verifyStringPropSet(WORKSPACE_URL)) {
            throw new DatabricksClientConfigException("Databricks Client Config missing "+ WORKSPACE_URL);
        } else if (!this.verifyStringPropSet(WORKSPACE_TOKEN) && !this.verifyStringPropSet(WORKSPACE_USERNAME)) {
            throw new DatabricksClientConfigException("Databricks Client Config missing either "+ WORKSPACE_TOKEN + " or " + WORKSPACE_USERNAME);
        } else if (!this.verifyStringPropSet(WORKSPACE_TOKEN) && this.verifyStringPropSet(WORKSPACE_USERNAME) && !this.verifyStringPropSet(WORKSPACE_PASSWORD)) {
            throw new DatabricksClientConfigException("Databricks Client Config missing " + WORKSPACE_PASSWORD);
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
        if (containsKey(key) && !getString(key).isEmpty()) {
            return true;
        }
        //log.info(key + " is not set");
        return false;
    }

    private boolean verifyStringPropNotSet(String key) {
        if (containsKey(key) && !getString(key).isEmpty()) {
            //log.info(key + " should not be set");
            return false;
        }
        return true;
    }
}
