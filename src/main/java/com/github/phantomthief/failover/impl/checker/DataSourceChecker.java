package com.github.phantomthief.failover.impl.checker;

import java.sql.Connection;

import javax.sql.DataSource;

/**
 * @author w.vela
 */
public class DataSourceChecker {

    public static boolean test(DataSource t) {
        try (Connection conn = t.getConnection()) {
            return conn != null;
        } catch (Throwable e) {
            return false;
        }
    }
}
