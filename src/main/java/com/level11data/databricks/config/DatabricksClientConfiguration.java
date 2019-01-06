package com.level11data.databricks.config;

import org.apache.commons.configuration2.PropertiesConfiguration;
import org.apache.commons.configuration2.ex.ConfigurationException;
import com.google.common.base.Preconditions;
import org.apache.log4j.Logger;

import java.io.*;
import java.net.URI;
import javax.ws.rs.core.UriBuilder;
public class DatabricksClientConfiguration extends PropertiesConfiguration {

    public static final String DEFAULT_RESOURCE_NAME = "databricks-client.properties";

    //private static final Logger log = Logger.getLogger(DatabricksClientConfiguration.class);

    public static final String LEVEL11DATA_PREFIX = "com.level11data";
    private static final String CLIENT_PREFIX = LEVEL11DATA_PREFIX + ".databricks.client";

    public static final String CLIENT_URL      = CLIENT_PREFIX + ".url";
    public static final String CLIENT_TOKEN    = CLIENT_PREFIX + ".token";
    public static final String CLIENT_USERNAME = CLIENT_PREFIX + ".username";
    public static final String CLIENT_PASSWORD = CLIENT_PREFIX + ".password";

    public DatabricksClientConfiguration() throws DatabricksClientConfigException {
        super();

        //attempt to read config from default resource file
        initConfigFromDefaultResource();

        //validate that config includes minimum required properties
        validateRequiredClientProps();
    }

    public DatabricksClientConfiguration(URI databricksURL, String token) throws DatabricksClientConfigException {
        super();

        this.addProperty(CLIENT_URL, databricksURL.toString());
        this.addProperty(CLIENT_TOKEN, token);

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

    public URI getClientUrl() {
        return UriBuilder.fromUri(getNonEmptyString(CLIENT_URL)).build();
    }

    public String getClientUsername() {
        return getNonEmptyString(CLIENT_USERNAME);
    }

    public String getClientPassword() {
        return getNonEmptyString(CLIENT_PASSWORD);
    }

    public String getClientToken() {
        return getNonEmptyString(CLIENT_TOKEN);
    }

    public boolean hasClientToken() {
        try {
            getClientToken();
            return true;
        } catch(NullPointerException e) {
            return false;
        } catch(IllegalArgumentException e) {
            return false;
        }
    }

    public boolean hasClientUsername() {
        try {
            getClientUsername();
            return true;
        } catch(NullPointerException e) {
            return false;
        } catch(IllegalArgumentException e) {
            return false;
        }
    }

    public boolean hasClientPassword() {
        try {
            getClientPassword();
            return true;
        } catch(NullPointerException e) {
            return false;
        } catch(IllegalArgumentException e) {
            return false;
        }
    }

    private void validateRequiredClientProps() throws DatabricksClientConfigException{
        boolean valid = true;
        valid &= verifyStringPropSet(CLIENT_URL);

        if(!verifyStringPropSet(CLIENT_URL)) {
            throw new DatabricksClientConfigException("Databricks Client Config missing "+CLIENT_URL);
        }


        if(verifyStringPropSet(CLIENT_TOKEN) || verifyStringPropSet(CLIENT_USERNAME)) {
            if(verifyStringPropSet(CLIENT_TOKEN)){
                //valid config
            } else if(verifyStringPropSet(CLIENT_USERNAME) && !verifyStringPropSet(CLIENT_PASSWORD)) {
                throw new DatabricksClientConfigException("Databricks Client Config missing " + CLIENT_PASSWORD);
            }

        } else {
            throw new DatabricksClientConfigException("Databricks Client Config missing either " +
                    CLIENT_TOKEN + " or " + CLIENT_USERNAME);
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