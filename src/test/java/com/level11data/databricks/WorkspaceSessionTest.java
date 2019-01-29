package com.level11data.databricks;

import com.level11data.databricks.cluster.ClusterConfigException;
import com.level11data.databricks.cluster.NodeType;
import com.level11data.databricks.cluster.SparkVersion;
import com.level11data.databricks.config.DatabricksClientConfiguration;
import com.level11data.databricks.session.WorkspaceSession;
import org.junit.Assert;
import org.junit.Test;

public class WorkspaceSessionTest {

    //load config from default resource databricks-client.properties (in test/resources)
    DatabricksClientConfiguration _databricksConfig = new DatabricksClientConfiguration();

    WorkspaceSession _databricks = new WorkspaceSession(_databricksConfig);

    String CONFIG_DEFAULT_NODE_TYPE = "com.level11data.databricks.client.default.cluster.nodeType";
    String CONFIG_DEFAULT_SPARK_VERSION = "com.level11data.databricks.client.default.cluster.sparkVersion";


    String _defaultNodeType = _databricksConfig.getString(CONFIG_DEFAULT_NODE_TYPE);
    String _defaultSparkVersion = _databricksConfig.getString(CONFIG_DEFAULT_SPARK_VERSION);


    public WorkspaceSessionTest() throws Exception {

    }


    @Test
    public void testZone() throws Exception {
        Assert.assertNotNull("No Default Zone", _databricks.getDefaultZone());
    }

    @Test
    public void testNodeType() throws Exception {
        NodeType firstNodeType = _databricks.getNodeTypes().get(0);

        Assert.assertNotNull("No Node Type Id", firstNodeType);

        NodeType defaultNodeType = _databricks.getNodeTypeById(_defaultNodeType);

        Assert.assertEquals("Default Node Type does not match",
                _defaultNodeType, defaultNodeType.NodeInstanceType.InstanceTypeId);

        try{
            _databricks.getNodeTypeById("FAIL");
        }catch(ClusterConfigException e) {
            Assert.assertEquals("Bad Node Type Did Not Throw Exception",
                   "No NodeType Found For Id FAIL",e.getMessage());
        }
    }

    @Test
    public void testSparkVersion() throws Exception {
        SparkVersion firstVersion = _databricks.getSparkVersions().get(0);
        Assert.assertNotNull("No Spark Version", firstVersion);

        SparkVersion defaultVersion = _databricks.getSparkVersionByKey(_defaultSparkVersion);
        Assert.assertEquals("Default Spark Version does not match",
                _defaultSparkVersion, defaultVersion.Key);

        try{
            _databricks.getSparkVersionByKey("FAIL");
        }catch(ClusterConfigException e) {
            Assert.assertEquals("Bad Node Type Did Not Throw Exception",
                    "No SparkVersion Found For Key FAIL",e.getMessage());
        }

    }


}
