package org.apache.kafka.server.difc;

public class DIFCConstants
{

    public static final String TAG_NAME_PATTERN = "^[A-Za-z0-9_-]+$";
    public static final String CLIENT_NAME_PATTERN = "^[A-Za-z0-9_-]+$";
    public static final int MIN_TAG_LENGTH = 4;
    public static final int MAX_TAG_LENGTH = 16;
    public static final int MIN_CLIENT_ID_LENGTH = 4;
    public static final int MAX_CLIENT_ID_LENGTH = 64;
    public static final int OK = 0;
}
