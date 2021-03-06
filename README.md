# Databricks Java Client

Unofficial Java client to the [Databricks REST API](https://docs.databricks.com/api/index.html).

## License

This project is released under the Apache License. See [LICENSE](./LICENSE) for more details.

## Build Process

### Build Client JAR

To build the client JAR, run the following [maven] command: 
  `mvn package`
  
To skip the tests, run the following [maven] command:
   `mvn package -Dmaven.test.skip=true`

### Running Tests
   
In order for the tests to run, the client needs to connect to a Databricks instance.
   
Add the following to your `.bash_profile` in order to set the required environment variables:
   
   `export DB_TEST_URL=<YOUR DATABRICKS WORKSPACE URL>`
   
Additionally, set either   
   `export DB_TEST_TOKEN=<YOUR DATABRICKS WORKSPACE ACCESS TOKEN>`
OR   
   `export DB_TEST_USER=<YOUR DATABRICKS WORKSPACE USERNAME>`
   `export DB_TEST_PASS=<YOUR DATABRICKS WORKSPACE PASSWORD>`
   
   
## Using the Client

### Create a WorkspaceSession

A `WorkspaceSession` is what establishes a connection between your client and a Databricks Workspace.

It can be instantiated in a number of different ways:

* With the URL and Token:
`WorkspaceSession(URI workspaceURL, String token)`

* With a *DatabricksClientConfiguration*
   `WorkspaceSession(DatabricksClientConfiguration databricksClientConfig)`

      See the section on [DatabricksClientConfiguration](###databricksclientconfiguration) for creating one

* With no arguments
   `WorkspaceSession()`
 
 This will use the default `DatabricksClientConfiguration`
 See the section on the [Default DatabricksClientConfiguration](####default-databricksclientconfiguration)


### DatabricksClientConfiguration
   The configuration is an extention of a regular java [PropertiesConfiguration](https://commons.apache.org/proper/commons-configuration/userguide/howto_properties.html) from the Apache Commons library.
   
   The following is the list or properties it expects:

| Property Name  | Default Value | Required |
|---|---|---|
| com.level11data.databricks.workspace.client.url  | None | Yes |
| com.level11data.databricks.workspace.client.token | None | Either token or username |
| com.level11data.databricks.workspace.client.username | None | Either username or token |
| com.level11data.databricks.workspace.client.password | None | Only with username |
      
The configuration can be instantiated a few different ways:

* With the URL and Token:
  `DatabricksClientConfiguration(URI workspaceURL, String token)`

* With a PropertiesFile reference:
  `DatabricksClientConfiguration(File propertiesFile)`

* With an InputStream of properties
  `DatabricksClientConfiguration(InputStream inputStream)`

* With a default configuration:
  `DatabricksClientConfiguration()`

#### Default DatabricksClientConfiguration
The default configuration will load the configuration from the default properties file, found in:
 [resources/databricks-client.properties](https://github.com/level11data/databricks-client-java/blob/master/src/main/resources/databricks-client.properties)

The default properties file will load the properties from the following environment variables.  If you set them in your `.bash_profile` file, they will be present for every terminal session:
* DB_URL
* DB_TOKEN
* DB_USER
* DB_PASS

