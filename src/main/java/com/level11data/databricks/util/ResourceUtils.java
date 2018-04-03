package com.level11data.databricks.util;

import com.level11data.databricks.client.DatabricksSession;
import com.level11data.databricks.client.HttpException;
import java.io.File;
import java.io.IOException;
import java.net.URI;

public class ResourceUtils {

    public static void validate(URI uri) throws ResourceConfigException {
        String scheme = uri.getScheme();
        boolean isValid = false;

        if(scheme == null) {
            throw new ResourceConfigException("Library must be stored in dbfs or s3. Make sure the URI begins with 'dbfs:' or 's3:'");
        } else if(scheme.equals("dbfs")) {
            isValid = true;
        } else if(scheme.equals("s3")) {
            isValid = true;
        } else if(scheme.equals("s3a")) {
            isValid = true;
        } else if(scheme.equals("s3n")) {
            isValid = true;
        }

        if(!isValid) {
            throw new ResourceConfigException(scheme + " is NOT a valid URI scheme");
        }
    }


    public static void uploadFile(DatabricksSession session, File file, URI destination) throws HttpException, IOException, ResourceConfigException {
        validate(destination);

        //TODO add support for s3, s3a, s3n, azure
        if(destination.getScheme().equals("dbfs")) {
            session.putDbfsFile(file, destination.toString());
        } else {
            throw new ResourceConfigException(destination.getScheme() + " is not a supported scheme for upload");
        }

    }
}
