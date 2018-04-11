package com.level11data.databricks.library;

import com.level11data.databricks.client.LibrariesClient;

public abstract class AbstractPublishedLibrary extends AbstractLibrary {

    public AbstractPublishedLibrary(LibrariesClient client) {
        super(client);
    }

}
