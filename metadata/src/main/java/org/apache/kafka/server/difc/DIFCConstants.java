package org.apache.kafka.server.difc;

public class DIFCConstants
{
    // Configuration constants
    public static final String TAG_NAME_PATTERN = "^[A-Za-z0-9_-]+$";
    public static final String CLIENT_NAME_PATTERN = "^[A-Za-z0-9_-]+$";
    public static final int MIN_TAG_LENGTH = 4;
    public static final int MAX_TAG_LENGTH = 16;
    public static final int MIN_CLIENT_ID_LENGTH = 4;
    public static final int MAX_CLIENT_ID_LENGTH = 64;

    // Status Codes
    public static final int OK = 0;
    public static final int ERR_NULL_INPUT = -1;
    public static final int ERR_INVALID_TAG_NAME = -2;
    public static final int ERR_INVALID_CLIENT_ID = -3;
    public static final int ERR_DUPLICATE_TAG = -4;
    public static final int ERR_TAG_NOT_FOUND = -5;
    public static final int ERR_CLIENT_EXISTS = -6;
    public static final int ERR_CLIENT_NOT_FOUND = -7;
    public static final int ERR_UNAUTHORIZED = -8;
    public static final int ERR_UNSUPPORTED_CAPABILITY = -9;
}
