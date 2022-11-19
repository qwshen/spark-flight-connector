package com.qwshen.flight.spark;

import com.qwshen.flight.Configuration;
import com.qwshen.flight.Table;
import org.apache.spark.sql.connector.catalog.TableProvider;
import org.apache.spark.sql.connector.expressions.Transform;
import org.apache.spark.sql.sources.DataSourceRegister;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;
import java.util.Map;

/**
 * Define the flight-source which supports both read from and write to remote flight-service
 */
public class FlightSource implements TableProvider, DataSourceRegister {
    //the option keys for connection
    private static final String HOST = "host";
    private static final String PORT = "port";
    private static final String TLS_ENABLED = "tls.enabled";
    private static final String TLS_VERIFY_SERVER = "tls.verifyServer";
    private static final String TLS_TRUSTSTORE_JKS = "tls.truststore.jksFile";
    private static final String TLS_TRUSTSTORE_PASS = "tls.truststore.pass";
    //the option keys for account
    private static final String USER = "user";
    private static final String PASSWORD = "password";
    private static final String BEARER_TOKEN = "bearerToken";

    //the option keys for table
    private static final String TABLE = "table";
    //the quote for field in case a field containing irregular characters, such as -
    private static final String COLUMN_QUOTE = "column.quote";

    //Managing Workloads
    public static final String KEY_DEFAULT_SCHEMA = "default.schema";
    public static final String KEY_ROUTING_TAG = "routing.tag";
    public static final String KEY_ROUTING_QUEUE = "routing.queue";

    //the service configuration
    private Configuration _configuration = null;
    //the name of the table
    private Table _table = null;

    /**
     * Infer schema from the options
     * @param options - the options container
     * @return - the schema inferred
     */
    @Override
    public StructType inferSchema(CaseInsensitiveStringMap options) {
        this.probeOptions(options);
        return this._table.getSparkSchema();
    }

    //extract all related options
    private void probeOptions(CaseInsensitiveStringMap options) {
        //host & port
        String host = options.getOrDefault(FlightSource.HOST, "");
        int port = Integer.parseInt(options.getOrDefault(FlightSource.PORT, "32010"));
        //account
        String user = options.getOrDefault(FlightSource.USER, "");
        String password = options.getOrDefault(FlightSource.PASSWORD, "");
        String bearerToken = options.getOrDefault(FlightSource.BEARER_TOKEN, "");
        //validation - host, user & password cannot be empty
        if (host.isEmpty() || user.isEmpty() || (password.isEmpty() && bearerToken.isEmpty())) {
            throw new RuntimeException("The host, user and (password or access-token) are all mandatory.");
        }
        //tls configuration
        boolean tlsEnabled = Boolean.parseBoolean(options.getOrDefault(FlightSource.TLS_ENABLED, "false"));
        boolean tlsVerify = Boolean.parseBoolean(options.getOrDefault(FlightSource.TLS_VERIFY_SERVER, "true"));
        String truststoreJks = options.getOrDefault(FlightSource.TLS_TRUSTSTORE_JKS, "");
        String truststorePass = options.getOrDefault(FlightSource.TLS_TRUSTSTORE_PASS, "");
        //set up the configuration object
        this._configuration = (truststoreJks != null && !truststoreJks.isEmpty())
            ? new Configuration(host, port, truststoreJks, truststorePass, user, password, bearerToken) : new Configuration(host, port, tlsEnabled, tlsEnabled && tlsVerify, user, password, bearerToken);

        //set the schema path, routing tag & queue if any to manage work-loads
        this._configuration.setDefaultSchema(options.getOrDefault(FlightSource.KEY_DEFAULT_SCHEMA, ""));
        this._configuration.setRoutingTag(options.getOrDefault(FlightSource.KEY_ROUTING_TAG, ""));
        this._configuration.setRoutingQueue(options.getOrDefault(FlightSource.KEY_ROUTING_QUEUE, ""));

        //the table name
        String tableName = options.getOrDefault(FlightSource.TABLE, "");
        //table name cannot empty
        if (tableName == null || tableName.isEmpty()) {
            throw new RuntimeException("The table is mandatory.");
        }
        //set up the flight-table with the column quote-character. By default, columns are not quoted
        this._table = Table.forTable(tableName, options.getOrDefault(FlightSource.COLUMN_QUOTE, ""));
        this._table.initialize(this._configuration);
    }

    /**
     * Get the table of the DataSource
     * @param schema - the schema of the table
     * @param partitioning - the partitioning for the table
     * @param properties - the properties of the table
     * @return - a Table object
     */
    @Override
    public org.apache.spark.sql.connector.catalog.Table getTable(StructType schema, Transform[] partitioning, Map<String, String> properties) {
        return new FlightTable(this._configuration, this._table);
    }

    /**
     * Get the short-name of the DataSource
     * @return - the short name of the DataSource
     */
    @Override
    public String shortName() {
        return "flight";
    }
}
