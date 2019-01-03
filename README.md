# Databricks Java Client

Unofficial Java client to the [Databricks REST API](https://docs.databricks.com/api/index.html).

## License

This project is released under the Apache License. See [LICENSE](./LICENSE) for more details.

## Build Process

###Build Client JAR

To build the client JAR, run the following [maven] command: 
  `mvn package`
  
To skip the tests, run the following [maven] command:
   `mvn package -Dmaven.test.skip=true`

###Running Tests
   
In order for the tests to run, the client needs to connect to a Databricks instance.
   
Add the following to your .bash_profile in order to set the required environment variables:
   
   `export DB_TEST_URL=<YOUR DATABRICKS URL>`
   
Additionally, set either   
   `export DB_TEST_TOKEN=<YOUR DATABRICKS ACCESS TOKEN>`
OR   
   `export DB_TEST_USER=<YOUR DATABRICKS USERNAME>`
   `export DB_TEST_PASS=<YOUR DATABRICKS PASSWORD>`
   
   
   