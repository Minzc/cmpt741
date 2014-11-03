package ca.sfu.dataming.util;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;

import java.util.Map;

/**
 * @author congzicun
 * @since 2014-11-02 3:16 PM
 */
public class DMConfiguration extends Configuration {
    private static final DMConfiguration config = new DMConfiguration();

    private DMConfiguration() {
        this.addResource("hbase-site.xml");
        this.addResource("core-site.xml");
        // this.addResource("dnsstat-config.xml");
        Configuration tmp = HBaseConfiguration.create();
        for (Map.Entry<String, String> stringStringEntry : tmp) {
            this.set(stringStringEntry.getKey(), stringStringEntry.getValue());
        }
    }

    public static DMConfiguration getInstance() {
        return config;
    }
}
