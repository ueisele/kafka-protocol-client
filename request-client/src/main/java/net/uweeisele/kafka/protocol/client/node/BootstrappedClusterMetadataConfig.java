package net.uweeisele.kafka.protocol.client.node;

import org.apache.kafka.clients.ClientDnsLookup;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigDef.Importance;
import org.apache.kafka.common.config.ConfigDef.Type;

import java.util.Map;
import java.util.Set;

import static org.apache.kafka.common.config.ConfigDef.Range.atLeast;
import static org.apache.kafka.common.config.ConfigDef.ValidString.in;

/**
 * The AdminClient configuration class, which also contains constants for configuration entry names.
 */
public class BootstrappedClusterMetadataConfig extends AbstractConfig {
    private static final ConfigDef CONFIG;

    /**
     * <code>bootstrap.servers</code>
     */
    public static final String BOOTSTRAP_SERVERS_CONFIG = CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG;
    private static final String BOOTSTRAP_SERVERS_DOC = CommonClientConfigs.BOOTSTRAP_SERVERS_DOC;

    /**
     * <code>client.dns.lookup</code>
     */
    public static final String CLIENT_DNS_LOOKUP_CONFIG = CommonClientConfigs.CLIENT_DNS_LOOKUP_CONFIG;
    private static final String CLIENT_DNS_LOOKUP_DOC = CommonClientConfigs.CLIENT_DNS_LOOKUP_DOC;

    /**
     * <code>retry.backoff.ms</code>
     */
    public static final String RETRY_BACKOFF_MS_CONFIG = CommonClientConfigs.RETRY_BACKOFF_MS_CONFIG;
    private static final String RETRY_BACKOFF_MS_DOC = "The amount of time to wait before attempting to " +
            "retry a failed request. This avoids repeatedly sending requests in a tight loop under " +
            "some failure scenarios.";

    public static final String METADATA_MAX_AGE_CONFIG = CommonClientConfigs.METADATA_MAX_AGE_CONFIG;
    private static final String METADATA_MAX_AGE_DOC = CommonClientConfigs.METADATA_MAX_AGE_DOC;

    static {
        CONFIG = new ConfigDef()
                .define(BOOTSTRAP_SERVERS_CONFIG,
                    Type.LIST,
                    Importance.HIGH,
                    BOOTSTRAP_SERVERS_DOC)
                .define(METADATA_MAX_AGE_CONFIG, Type.LONG, 5 * 60 * 1000, atLeast(0), Importance.LOW, METADATA_MAX_AGE_DOC)
                .define(RETRY_BACKOFF_MS_CONFIG,
                        Type.LONG,
                        100L,
                        atLeast(0L),
                        Importance.LOW,
                        RETRY_BACKOFF_MS_DOC)
                .define(CLIENT_DNS_LOOKUP_CONFIG,
                        Type.STRING,
                        ClientDnsLookup.DEFAULT.toString(),
                        in(ClientDnsLookup.DEFAULT.toString(),
                                ClientDnsLookup.USE_ALL_DNS_IPS.toString(),
                                ClientDnsLookup.RESOLVE_CANONICAL_BOOTSTRAP_SERVERS_ONLY.toString()),
                        Importance.MEDIUM,
                        CLIENT_DNS_LOOKUP_DOC);
    }

    @Override
    protected Map<String, Object> postProcessParsedConfig(final Map<String, Object> parsedValues) {
        return CommonClientConfigs.postProcessReconnectBackoffConfigs(this, parsedValues);
    }

    public BootstrappedClusterMetadataConfig(Map<?, ?> props) {
        this(props, false);
    }

    protected BootstrappedClusterMetadataConfig(Map<?, ?> props, boolean doLog) {
        super(CONFIG, props, doLog);
    }

    public static Set<String> configNames() {
        return CONFIG.names();
    }

    public static ConfigDef configDef() {
        return  new ConfigDef(CONFIG);
    }

    public static void main(String[] args) {
        System.out.println(CONFIG.toHtmlTable());
    }

}