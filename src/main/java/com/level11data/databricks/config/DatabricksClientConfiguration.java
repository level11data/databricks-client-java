package com.level11data.databricks.config;

import org.apache.commons.configuration2.PropertiesConfiguration;
import org.apache.commons.configuration2.ex.ConfigurationException;
import com.google.common.base.Preconditions;

import java.io.*;
import java.net.URI;
import javax.ws.rs.core.UriBuilder;
public class DatabricksClientConfiguration extends PropertiesConfiguration {

    public static final String DEFAULT_RESOURCE_NAME = "databricks-client.properties";

    //private static final Logger log = Logger.getLogger(DatabricksClientConfiguration.class);

    public static final String LEVEL11DATA_PREFIX = "com.level11data";
    private static final String CLIENT_PREFIX = LEVEL11DATA_PREFIX + ".databricks.client";
    private static final String WORKSPACE_PREFIX = CLIENT_PREFIX + ".workspace";
    public static final String WORKSPACE_URL      = WORKSPACE_PREFIX + ".url";
    public static final String WORKSPACE_TOKEN    = WORKSPACE_PREFIX + ".token";
    public static final String WORKSPACE_USERNAME = WORKSPACE_PREFIX + ".username";
    public static final String WORKSPACE_PASSWORD = WORKSPACE_PREFIX + ".password";
    public String userAgent = "infoworks.io";

    public DatabricksClientConfiguration() throws DatabricksClientConfigException {
        super();

        //attempt to read config from default resource file
        initConfigFromDefaultResource();

        //validate that config includes minimum required properties
        validateRequiredClientProps();
    }

    public DatabricksClientConfiguration(URI databricksURL, String token) throws DatabricksClientConfigException {
        super();

        this.addProperty(WORKSPACE_URL, databricksURL.toString());
        this.addProperty(WORKSPACE_TOKEN, token);

        //validate that config includes minimum required properties
        validateRequiredClientProps();
    }


    public DatabricksClientConfiguration(File propertiesFile) throws DatabricksClientConfigException {
        super();
        FileInputStream inputStream;
        try{
            inputStream = new FileInputStream(propertiesFile);
        } catch(FileNotFoundException e) {
            throw new DatabricksClientConfigException("File Not Found: "+propertiesFile.getAbsolutePath(), e);
        }

        readConfigFromStream(inputStream);

        try{
            inputStream.close();
        } catch(IOException e) {
            throw new DatabricksClientConfigException(e);
        }

        //validate that config includes minimum required properties
        validateRequiredClientProps();
    }

    public DatabricksClientConfiguration(InputStream inputStream) throws DatabricksClientConfigException {
        super();
        readConfigFromStream(inputStream);

        //validate that config includes minimum required properties
        validateRequiredClientProps();
    }

    private void initConfigFromDefaultResource() throws DatabricksClientConfigException {
        ClassLoader loader = Thread.currentThread().getContextClassLoader();
        InputStream resourceStream = loader.getResourceAsStream(DEFAULT_RESOURCE_NAME);

        if (resourceStream == null) {
            throw new DatabricksClientConfigException("Default Resource Not Found: " + DEFAULT_RESOURCE_NAME);
        }
        readConfigFromStream(resourceStream);
        try{
            resourceStream.close();
        } catch(IOException e) {
            throw new DatabricksClientConfigException(e);
        }
    }

    private void readConfigFromStream(InputStream inputStream) throws DatabricksClientConfigException {
        try {
            this.read(new InputStreamReader(inputStream));
        } catch(IOException e) {
            throw new DatabricksClientConfigException(e);
        } catch (ConfigurationException e) {
            throw new DatabricksClientConfigException(e);
        }
    }

    public URI getWorkspaceUrl() {
        return UriBuilder.fromUri(getNonEmptyString(WORKSPACE_URL)).build();
    }

    public String getWorkspaceUsername() {
        return getNonEmptyString(WORKSPACE_USERNAME);
    }

    public String getWorkspacePassword() {
        return getNonEmptyString(WORKSPACE_PASSWORD);
    }

    public String getWorkspaceToken() {
        return getNonEmptyString(WORKSPACE_TOKEN);
    }

    public boolean hasClientToken() {
        try {
            getWorkspaceToken();
            return true;
        } catch(NullPointerException e) {
            return false;
        } catch(IllegalArgumentException e) {
            return false;
        }
    }

    public boolean hasClientUsername() {
        try {
            getWorkspaceUsername();
            return true;
        } catch(NullPointerException e) {
            return false;
        } catch(IllegalArgumentException e) {
            return false;
        }
    }

    public boolean hasClientPassword() {
        try {
            getWorkspacePassword();
            return true;
        } catch(NullPointerException e) {
            return false;
        } catch(IllegalArgumentException e) {
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

        if(!verifyStringPropSet(WORKSPACE_URL)) {
            throw new DatabricksClientConfigException("Databricks Client Config missing " + WORKSPACE_URL);
        }


        if(verifyStringPropSet(WORKSPACE_TOKEN) || verifyStringPropSet(WORKSPACE_USERNAME)) {
            if(verifyStringPropSet(WORKSPACE_TOKEN)){
                //valid config
            } else if(verifyStringPropSet(WORKSPACE_USERNAME) && !verifyStringPropSet(WORKSPACE_PASSWORD)) {
                throw new DatabricksClientConfigException("Databricks Client Config missing " + WORKSPACE_PASSWORD);
            }

        } else {
            throw new DatabricksClientConfigException("Databricks Client Config missing either " +
                    WORKSPACE_TOKEN + " or " + WORKSPACE_USERNAME);
        }
    }

    private String getNonEmptyString(String property, String defaultValue) {
        String value = getString(property, defaultValue);
        Preconditions.checkNotNull(value, property + " cannot be null");
        Preconditions.checkArgument(!value.isEmpty(), property + " cannot be empty");
        return value;
    }

    private String getNonEmptyString(String property) {
        String value = getString(property);
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
