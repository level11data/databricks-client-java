package com.level11data.databricks.library;

import com.level11data.databricks.client.LibrariesClient;

public abstract class PublishedLibrary extends Library {

    public PublishedLibrary(LibrariesClient client) {
        super(client);
    }

}
