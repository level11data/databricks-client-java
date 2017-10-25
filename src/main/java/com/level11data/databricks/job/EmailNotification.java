package com.level11data.databricks.job;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class EmailNotification {
    public final List<String> EmailAddresses = Collections.unmodifiableList(new ArrayList<String>());

    public EmailNotification(String emailAddress) {
        EmailAddresses.add(emailAddress);
    }

    public EmailNotification(String[] emailAddress) {
        for (int i = 0; i < emailAddress.length; i++){
            EmailAddresses.add(emailAddress[i]);
        }
    }
}
