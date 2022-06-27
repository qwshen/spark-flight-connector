package com.qwshen.flight;

/**
 * The protocol tells the connector how to conduct the write operation
 */
public enum WriteProtocol {
    //literal sql statements are submitted
    SQL,
    //prepared sql statements are submitted
    ARROW
}
