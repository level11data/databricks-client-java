package com.level11data.databricks.library;

public enum LibraryInstallStatus {
    PENDING, RESOLVING, INSTALLING, INSTALLED, FAILED, UNINSTALL_ON_RESTART;

    public boolean isFinal() {
        if(this == LibraryInstallStatus.INSTALLED) return true;
        if(this == LibraryInstallStatus.FAILED) return true;
        if(this == LibraryInstallStatus.UNINSTALL_ON_RESTART) return true;
        return false;
    }

}
