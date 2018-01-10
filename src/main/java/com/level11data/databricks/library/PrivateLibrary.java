package com.level11data.databricks.library;

import com.level11data.databricks.client.HttpException;
import com.level11data.databricks.client.LibrariesClient;
import com.level11data.databricks.util.FileUtils;

import java.io.File;
import java.io.IOException;
import java.net.URI;

public abstract class PrivateLibrary extends Library {
    private final LibrariesClient _client;

    public final URI Uri;

    public PrivateLibrary(LibrariesClient client, URI uri) throws LibraryConfigException {
        super();
        _client = client;
        validate(uri);
        Uri = uri;
    }

    private void validate(URI uri) throws LibraryConfigException {
        String scheme = uri.getScheme();
        boolean isValid = false;

        if(scheme == null) {
            throw new LibraryConfigException("Library must be stored in dbfs or s3. Make sure the URI begins with 'dbfs:' or 's3:'");
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
            throw new LibraryConfigException(scheme + " is NOT a valid URI scheme");
        }
    }

    public void upload(File file) throws HttpException, IOException, LibraryConfigException {
        FileUtils.uploadFile(_client.Session, file, Uri);
    }
}
