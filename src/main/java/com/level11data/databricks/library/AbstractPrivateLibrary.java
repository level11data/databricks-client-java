package com.level11data.databricks.library;

import com.level11data.databricks.client.HttpException;
import com.level11data.databricks.client.LibrariesClient;
import com.level11data.databricks.util.ResourceConfigException;
import com.level11data.databricks.util.ResourceUtils;

import java.io.File;
import java.io.IOException;
import java.net.URI;

public abstract class AbstractPrivateLibrary extends AbstractLibrary {
    private final LibrariesClient _client;

    public final URI Uri;

    public AbstractPrivateLibrary(LibrariesClient client, URI uri) throws LibraryConfigException {
        super(client);
        _client = client;
        validate(uri);
        Uri = uri;
    }

    private void validate(URI uri) throws LibraryConfigException {
        try {
            ResourceUtils.validate(uri);
        } catch (ResourceConfigException e) {
            throw new LibraryConfigException(e);
        }
    }

    public void upload(File file) throws LibraryConfigException {
        try {
            ResourceUtils.uploadFile(_client.Session, file, Uri);
        } catch(ResourceConfigException e) {
            throw new LibraryConfigException(e);
        } catch(HttpException e) {
            throw new LibraryConfigException(e);
        } catch(IOException e) {
            throw new LibraryConfigException(e);
        }
    }
}
