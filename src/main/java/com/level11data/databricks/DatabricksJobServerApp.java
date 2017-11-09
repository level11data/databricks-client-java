package com.level11data.databricks;


import com.level11data.databricks.cluster.InteractiveCluster;
import com.level11data.databricks.cluster.ClusterState;
import com.level11data.databricks.config.DatabricksClientConfiguration;
import com.level11data.databricks.entities.commands.CommandRequestDTO;
import com.level11data.databricks.entities.commands.ExecuteCommandRequestDTO;
import com.level11data.databricks.entities.commands.CommandStatusDTO;
import com.level11data.databricks.entities.contexts.CreateContextRequestDTO;

import java.io.InputStream;

public class DatabricksJobServerApp {
    public static final String CLIENT_CONFIG_RESOURCE_NAME = "databricks-client.properties";

    public static void main(String[] args) {
        System.out.println("DatabricksClient- Begin Reading Resource");

        ClassLoader loader = Thread.currentThread().getContextClassLoader();
        InputStream resourceStream = loader.getResourceAsStream(CLIENT_CONFIG_RESOURCE_NAME);
        if(resourceStream == null) {
            throw new IllegalArgumentException("Resource Not Found: " + CLIENT_CONFIG_RESOURCE_NAME);
        }

        System.out.println("Resource Loaded...");
        try {
            DatabricksClientConfiguration databricksConfig = new DatabricksClientConfiguration(resourceStream);
            DatabricksSession databricks = new DatabricksSession(databricksConfig);

            System.out.println("Username: " + databricksConfig.getClientUsername());
            System.out.println("URL: " + databricksConfig.getClientUrl());

            ClustersClient cClient = new ClustersClient(databricks);

            InteractiveCluster interactiveCluster;

            interactiveCluster = databricks.createCluster("My JobServer InteractiveCluster", 1)
                    .withAutoTerminationMinutes(25)
                    .withNodeType("i3.xlarge")
                    .withSparkVersion("3.3.x-scala2.11")
                    .create();


            while(interactiveCluster.getState() == ClusterState.PENDING) {
                //wait until cluster is properly started
                // should not take more than 100 seconds from a cold start
                System.out.println("InteractiveCluster is starting...");
                Thread.sleep(10000); //wait 10 seconds
            }


            //interactiveCluster = databricks.getCluster("0927-044941-grove655");

            //create scala context
            ContextsClient contextClient = new ContextsClient(databricks);
            CreateContextRequestDTO createContextRequest = new CreateContextRequestDTO();
            createContextRequest.ClusterId = interactiveCluster.Id;
            createContextRequest.Language = "scala";
            long contextId = contextClient.createContext(createContextRequest);

            //command request
            CommandsClient cmdClient = new CommandsClient(databricks);
            System.out.println("Executing Command...");
            ExecuteCommandRequestDTO command = new ExecuteCommandRequestDTO();
            command.Language = "scala";
            command.ClusterId = interactiveCluster.Id;
            command.ContextId = contextId;
            command.Command = "println(\"Hello World!!\")"; //This command could be anything
            System.out.println("ExecuteCommandRequest="+command.toString());

            String commandId = cmdClient.executeCommand(command);

            System.out.println("Checking Command Status...");
            CommandRequestDTO commandRequest = new CommandRequestDTO();
            commandRequest.ClusterId = interactiveCluster.Id;
            commandRequest.ContextId = interactiveCluster.SparkContextId;
            commandRequest.CommandId = commandId;

            System.out.println("CommandRequest="+commandRequest.toString());

            CommandStatusDTO cmdStatus = cmdClient.getCommandStatus(commandRequest);

            while(cmdStatus.Status.equals("Running")) {
                //wait until command is executed
                System.out.println("Command is "+cmdStatus.Status);
                Thread.sleep(1000); //wait 1 second
                cmdStatus = cmdClient.getCommandStatus(commandRequest);
            }
            System.out.println("Command Results: "+cmdStatus.toString());
        } catch (Exception e) {
            System.out.println("Error Exception Thrown: "+ e);
            e.printStackTrace();
        }
    }
}
