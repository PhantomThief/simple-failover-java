/**
 * 
 */
package com.github.phantomthief.failover.impl.checker;

import java.sql.Connection;

import javax.sql.DataSource;

/**
 * @author w.vela
 */
public class DataSourceChecker {

    /* (non-Javadoc)
     * @see java.util.function.Predicate#test(java.lang.Object)
     */
    public static boolean test(DataSource t) {
        try (Connection conn = t.getConnection()) {
            return conn != null;
        } catch (Throwable e) {
            return false;
        }
    }

}
