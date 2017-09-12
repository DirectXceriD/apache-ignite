package org.apache.ignite.sqltester;

import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteJdbcDriver;
import org.apache.ignite.Ignition;
import org.apache.ignite.internal.util.typedef.F;

/**
 *
 */
public class IgniteQueryTestRunner implements QueryTestRunner {

    private final String type = "ignite";

    private Ignite ignite;

    @Override public String getType() {
        return type;
    }

    @Override public void beforeTest(QueryTypeConfiguration cfg) {
        String cfgPath = F.isEmpty((String) cfg.getIgniteCfgPath()) ? "ignite-localhost-config.xml" :
                (String) cfg.getIgniteCfgPath();

        ignite = Ignition.start(cfgPath);
    }

    @Override public String driverClassName() {
        return IgniteJdbcDriver.class.getName();
    }

    @Override public void afterTest() {
        ignite.close();
    }
}
