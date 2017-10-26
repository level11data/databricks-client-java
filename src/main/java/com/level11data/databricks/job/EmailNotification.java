package com.level11data.databricks.job;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class EmailNotification {
    public final List<String> EmailAddresses;

    public EmailNotification(String emailAddress) {
        ArrayList<String> newList = new ArrayList<String>();
        newList.add(emailAddress);
        EmailAddresses = Collections.unmodifiableList(newList);
    }

    public EmailNotification(String[] emailAddress) {
        ArrayList<String> newList = new ArrayList<String>();

        for (int i = 0; i < emailAddress.length; i++){
            newList.add(emailAddress[i]);
        }
        EmailAddresses = Collections.unmodifiableList(newList);
    }
}
