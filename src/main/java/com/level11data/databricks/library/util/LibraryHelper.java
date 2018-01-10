package com.level11data.databricks.library.util;

import com.level11data.databricks.client.LibrariesClient;
import com.level11data.databricks.client.entities.libraries.LibraryDTO;
import com.level11data.databricks.library.*;

import java.net.URI;
import java.net.URISyntaxException;

public class LibraryHelper {

    public static Library createLibrary(LibrariesClient librariesClient, LibraryDTO libraryDTO)
            throws LibraryConfigException, URISyntaxException {
        if(libraryDTO.Jar != null) {
            return new JarLibrary(librariesClient, new URI(libraryDTO.Jar));
        } else if(libraryDTO.Egg != null) {
            return new EggLibrary(librariesClient, new URI(libraryDTO.Egg));
        } else if (libraryDTO.Maven != null) {
            return new MavenLibrary(librariesClient,
                    libraryDTO.Maven.Coordinates,
                    libraryDTO.Maven.Repo,
                    libraryDTO.Maven.Exclusions);
        } else if (libraryDTO.PyPi != null) {
            return new PyPiLibrary(librariesClient,
                    libraryDTO.PyPi.Package,
                    libraryDTO.PyPi.Repo);
        } else if (libraryDTO.Cran != null) {
            return new CranLibrary(librariesClient,
                    libraryDTO.Cran.Package,
                    libraryDTO.Cran.Repo);
        } else {
            throw new LibraryConfigException("Unknown Library Type");
        }
    }
}
