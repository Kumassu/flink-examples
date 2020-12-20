package org.example.flink.model;

import java.util.Random;

public class Gender {
    public static final String MALE = "MALE";
    public static final String FEMALE = "FEMALE";



    public static String random() {
        int nextInt = new Random().nextInt(2);
        return nextInt == 0 ? MALE : FEMALE;
    }
}