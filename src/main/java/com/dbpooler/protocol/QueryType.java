package com.dbpooler.protocol;

/**
 * Classifies a SQL query for routing purposes.
 * READ queries go to replica pools, WRITE queries go to the master pool.
 */
public enum QueryType {
    READ,
    WRITE,
    UNKNOWN
}
