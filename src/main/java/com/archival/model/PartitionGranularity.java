package com.archival.model;

/**
 * How output Parquet files are grouped/split by the {@code fye} value within
 * a single job's export window. See requirement: "Group/split output by fye
 * where appropriate".
 */
public enum PartitionGranularity {
    /** One Parquet file per exact fye date value. */
    EXACT_DATE,
    /** One Parquet file per calendar month of fye (default). */
    MONTH,
    /** One Parquet file per calendar year of fye. */
    YEAR,
    /** No grouping - a single Parquet file for the whole window. */
    NONE
}
