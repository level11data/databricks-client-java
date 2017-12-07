package com.level11data.databricks.library;

import java.net.URI;

public abstract class Library {
    public final URI Uri;

    public Library(URI uri) throws LibraryConfigException {
        validate(uri);
        Uri = uri;
    }

    private void validate(URI uri) throws LibraryConfigException {
        String scheme = uri.getScheme();
        boolean isValid = false;

        if(scheme.equals("dbfs")) {
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
}
