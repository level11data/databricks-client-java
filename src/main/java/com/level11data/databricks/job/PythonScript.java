package com.level11data.databricks.job;

import com.level11data.databricks.client.DatabricksSession;
import com.level11data.databricks.util.ResourceConfigException;
import com.level11data.databricks.client.HttpException;
import com.level11data.databricks.util.ResourceUtils;

import java.io.File;
import java.io.IOException;
import java.net.URI;

public class PythonScript {
    private final DatabricksSession _session;

    public final URI Uri;

    public PythonScript(DatabricksSession session, URI uri) throws ResourceConfigException {
        _session = session;
        Uri = uri;
        ResourceUtils.validate(uri);
    }

    public void upload(File file) throws HttpException, IOException, ResourceConfigException {
        ResourceUtils.uploadFile(_session, file, Uri);
    }
}
