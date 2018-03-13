package com.level11data.databricks.config;

import org.apache.commons.configuration2.AbstractConfiguration;
import org.apache.commons.configuration2.CompositeConfiguration;
import org.apache.commons.configuration2.Configuration;
import org.apache.commons.configuration2.PropertiesConfiguration;
import org.apache.commons.configuration2.ex.ConfigurationException;
import com.google.common.base.Preconditions;
import org.apache.log4j.Logger;

import java.io.*;
import java.net.URI;
import java.util.Iterator;
import javax.ws.rs.core.UriBuilder;
/*
Inspired by
  http://www.programcreek.com/java-api-examples/index.php?source_dir=fluo-master/modules/api/src/main/java/io/fluo/api/config/FluoConfiguration.java

*/

public class DatabricksClientConfiguration extends CompositeConfiguration {

    private static final Logger log = Logger.getLogger(DatabricksClientConfiguration.class);

    public static final String LEVEL11DATA_PREFIX = "com.level11data";
    private static final String CLIENT_PREFIX = LEVEL11DATA_PREFIX + ".databricks.client";
    public static final String CLIENT_TOKEN = CLIENT_PREFIX + ".token";
    public static final String CLIENT_USERNAME = CLIENT_PREFIX + ".username";
    public static final String CLIENT_PASSWORD = CLIENT_PREFIX + ".password";
    public static final String CLIENT_URL = CLIENT_PREFIX + ".url";

    public DatabricksClientConfiguration() {
        super();
        setThrowExceptionOnMissing(true);
    }

    public DatabricksClientConfiguration(DatabricksClientConfiguration other) {
        this();
        Iterator<String> iter = other.getKeys();
        while (iter.hasNext()) {
            String key = iter.next();
            setProperty(key, other.getProperty(key));
        }
    }

    public DatabricksClientConfiguration(Configuration configuration) {
        this();
        if (configuration instanceof AbstractConfiguration) {
            AbstractConfiguration aconf = (AbstractConfiguration) configuration;
        }
        addConfiguration(configuration);
    }

    public DatabricksClientConfiguration(File propertiesFile) throws IllegalArgumentException {
        this();
        try {
            Reader reader = new FileReader(propertiesFile);
            PropertiesConfiguration config = new PropertiesConfiguration();
            config.read(reader);
            addConfiguration(config);
        } catch (FileNotFoundException e) {
            throw new IllegalArgumentException("Configuration File Not Found: " + propertiesFile.getAbsolutePath());
        } catch(IOException e) {
            throw new IllegalArgumentException(("File could not be read: " + propertiesFile.getAbsolutePath()));
        } catch (ConfigurationException e) {
            throw new IllegalArgumentException(e);
        }
    }

    public DatabricksClientConfiguration(InputStream inputStream) throws IOException, ConfigurationException {
        this();
        try {
            PropertiesConfiguration config = new PropertiesConfiguration();
            config.read(new InputStreamReader(inputStream));
            addConfiguration(config);
        } catch(IOException e) {
            throw new IllegalArgumentException(e);
        } catch (ConfigurationException e) {
            throw new IllegalArgumentException(e);
        }
    }

    public Configuration getClientConfiguration() {
        Configuration clientConfig = new CompositeConfiguration();
        Iterator<String> iter = getKeys();
        while (iter.hasNext()) {
            String key = iter.next();
            if (key.startsWith(CLIENT_PREFIX)) {
                clientConfig.setProperty(key, getProperty(key));
            }
        }
        return clientConfig;
    }

    public void validate() {
        // keep in alphabetical order
        getClientPassword();
        getClientUrl();
        getClientUsername();
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

    /**
     * Returns true if required properties for Client are set
     */
    public boolean hasRequiredClientProps() {
        boolean valid = true;
        valid &= verifyStringPropSet(CLIENT_URL);

        if(verifyStringPropSet(CLIENT_TOKEN)) {
            valid &= true;
        } else if(verifyStringPropSet(CLIENT_USERNAME) & verifyStringPropSet(CLIENT_PASSWORD)) {
            valid &= true;
        }
        return valid;
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
        log.info(key + " is not set");
        return false;
    }

    private boolean verifyStringPropNotSet(String key) {
        if (containsKey(key) && !getString(key).isEmpty()) {
            log.info(key + " should not be set");
            return false;
        }
        return true;
    }
}