package edu.njit.cs643.group3.utils;

import java.time.format.DateTimeFormatter;

public class Constants {
    public final static DateTimeFormatter TIMESTAMP_FORMAT = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
    public final static DateTimeFormatter TIME_FORMAT = DateTimeFormatter.ofPattern("HH:mm");

    public final static String PATH_INPUT = "/input";
    public final static String PATH_OUTPUT = "/output";

    private Constants() { }
}
