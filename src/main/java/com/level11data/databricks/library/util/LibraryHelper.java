package com.level11data.databricks.library.util;

import com.level11data.databricks.client.LibrariesClient;
import com.level11data.databricks.client.entities.libraries.LibraryDTO;
import com.level11data.databricks.client.entities.libraries.MavenLibraryDTO;
import com.level11data.databricks.client.entities.libraries.PythonPyPiLibraryDTO;
import com.level11data.databricks.client.entities.libraries.RCranLibraryDTO;
import com.level11data.databricks.library.*;

import java.net.URI;
import java.net.URISyntaxException;

public class LibraryHelper {

    public static AbstractLibrary createLibrary(LibrariesClient librariesClient, LibraryDTO libraryDTO) throws LibraryConfigException {
        try {
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
                throw new LibraryConfigException("Unknown AbstractLibrary Type");
            }
        } catch (URISyntaxException e) {
            throw new LibraryConfigException(e);
        }
    }

    public static LibraryDTO createJarLibraryDTO(URI uri) {
        LibraryDTO libraryDTO = new LibraryDTO();
        libraryDTO.Jar = uri.toString();
        return libraryDTO;
    }


    public static LibraryDTO createEggLibraryDTO(URI uri) {
        LibraryDTO libraryDTO = new LibraryDTO();
        libraryDTO.Egg = uri.toString();
        return libraryDTO;
    }

    public static LibraryDTO createMavenLibraryDTO(String coordinates, String repo, String[] exclusions) {
        LibraryDTO libraryDTO = new LibraryDTO();
        MavenLibraryDTO mavenDTO = new MavenLibraryDTO();
        mavenDTO.Coordinates = coordinates;

        if(repo != null) mavenDTO.Repo = repo;
        if(exclusions != null) mavenDTO.Exclusions = exclusions;

        libraryDTO.Maven = mavenDTO;
        return libraryDTO;
    }

    public static LibraryDTO createMavenLibraryDTO(String coordinates) {
        return createMavenLibraryDTO(coordinates, null, null);
    }

    public static LibraryDTO createMavenLibraryDTO(String coordinates, String repo) {
        return createMavenLibraryDTO(coordinates, repo, null);
    }

    public static LibraryDTO createMavenLibraryDTO(String coordinates, String[] exclusions) {
        return createMavenLibraryDTO(coordinates, null, exclusions);
    }

    public static LibraryDTO createPyPiLibraryDTO(String packageName, String repo) {
        LibraryDTO libraryDTO = new LibraryDTO();
        PythonPyPiLibraryDTO piPyDTO = new PythonPyPiLibraryDTO();
        piPyDTO.Package = packageName;

        if(repo != null) piPyDTO.Repo = repo;

        libraryDTO.PyPi = piPyDTO;
        return libraryDTO;
    }

    public static LibraryDTO createPyPiLibraryDTO(String packageName)  {
        return createPyPiLibraryDTO(packageName, null);
    }

    public static LibraryDTO createCranLibraryDTO(String packageName) {
        return createCranLibraryDTO(packageName, null);
    }

    public static LibraryDTO createCranLibraryDTO(String packageName, String repo) {
        LibraryDTO libraryDTO = new LibraryDTO();
        RCranLibraryDTO cranDTO = new RCranLibraryDTO();
        cranDTO.Package = packageName;
        if(repo != null) cranDTO.Repo = repo;
        libraryDTO.Cran = cranDTO;
        return libraryDTO;
    }

    public static LibraryDTO createLibraryDTO(AbstractLibrary library) {
        LibraryDTO libraryDTO = new LibraryDTO();

        if(library instanceof JarLibrary) {
            libraryDTO.Jar = ((JarLibrary)library).Uri.toString();

        } else if (library instanceof EggLibrary) {
            libraryDTO.Jar = ((EggLibrary)library).Uri.toString();

        } else if (library instanceof MavenLibrary) {
            MavenLibraryDTO mavenLibraryDTO = new MavenLibraryDTO();
            mavenLibraryDTO.Coordinates = ((MavenLibrary)library).Coordinates;
            mavenLibraryDTO.Repo = ((MavenLibrary)library).RepoOverride;
            mavenLibraryDTO.Exclusions = ((MavenLibrary)library).DependencyExclusions;
            libraryDTO.Maven = mavenLibraryDTO;

        } else if (library instanceof PyPiLibrary) {
            PythonPyPiLibraryDTO pyPiLibrary = new PythonPyPiLibraryDTO();
            pyPiLibrary.Package = ((PyPiLibrary)library).PackageName;
            pyPiLibrary.Repo = ((PyPiLibrary)library).RepoOverride;
            libraryDTO.PyPi = pyPiLibrary;

        } else if (library instanceof CranLibrary) {
            RCranLibraryDTO cranLibraryDTO = new RCranLibraryDTO();
            cranLibraryDTO.Package = ((CranLibrary)library).PackageName;
            cranLibraryDTO.Repo = ((CranLibrary)library).RepoOverride;
            libraryDTO.Cran = cranLibraryDTO;

        }
        return libraryDTO;
    }


}
