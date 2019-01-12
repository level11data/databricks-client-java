package com.level11data.databricks.util;

import java.io.File;

public class TestUtils {

    public static File getResourceByName(String resourceName) {
        ClassLoader loader = Thread.currentThread().getContextClassLoader();
        String localPath = loader.getResource(resourceName).getFile();
        return new File(localPath);
    }
}
