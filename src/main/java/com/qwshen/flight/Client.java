package com.qwshen.flight;

import org.apache.arrow.flight.*;
import org.apache.arrow.flight.auth2.BasicAuthCredentialWriter;
import org.apache.arrow.flight.auth2.BearerCredentialWriter;
import org.apache.arrow.flight.auth2.ClientBearerHeaderHandler;
import org.apache.arrow.flight.auth2.ClientIncomingAuthHeaderMiddleware;
import org.apache.arrow.flight.grpc.CredentialCallOption;
import org.apache.arrow.flight.sql.FlightSqlClient;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.util.AutoCloseables;
import org.apache.arrow.vector.*;
import org.apache.arrow.vector.types.pojo.Schema;
import org.slf4j.LoggerFactory;
import java.io.ByteArrayInputStream;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;

/**
 * Describes the data-structure of Client for communicating with remote flight service
 */
public final class Client implements AutoCloseable {
    //the factory
    private static final ClientIncomingAuthHeaderMiddleware.Factory _factory = new ClientIncomingAuthHeaderMiddleware.Factory(new ClientBearerHeaderHandler());
    //the existing objects of client
    private static final java.util.Map<String, Client> _clients = new java.util.HashMap<>();

    //the flight client
    private final FlightClient _client;
    //the flight-sql client
    private final FlightSqlClient _sqlClient;
    //the token for calls
    private final CredentialCallOption _bearerToken;

    //the buffer
    private final BufferAllocator _allocator;
    //the connection string to identify the client
    private final String _connectionString;

    /**
     * Construct a Client object
     * @param client - the client object of the flight service
     * @param bearerToken - the credential token
     * @param connectionString - the connection string to identify the client
     */
    private Client(FlightClient client, CredentialCallOption bearerToken, String connectionString, BufferAllocator allocator) {
        this._client = client;
        this._sqlClient = new FlightSqlClient(this._client);
        this._bearerToken = bearerToken;

        this._connectionString = connectionString;
        this._allocator = allocator;
    }

    /**
     * Fetch meta-data of all related end-points by a query
     * @param query - the query submitted to remote flight-service
     * @return - the schema and end-points for the query
     */
    public QueryEndpoints getQueryEndpoints(String query) {
        FlightInfo fi = this._client.getInfo(FlightDescriptor.command(query.getBytes(StandardCharsets.UTF_8)), this._bearerToken);
        Endpoint[] endpoints = fi.getEndpoints().stream().map(ep -> new Endpoint(ep.getLocations().stream().map(Location::getUri).toArray(URI[]::new), ep.getTicket().getBytes())).toArray(Endpoint[]::new);
        return new QueryEndpoints(fi.getSchema(), endpoints);
    }

    /**
     * Fetch rows from the end-point
     * @param ep - the end-point
     * @param schema - the schema of rows from the end-point
     * @return - row set from the end-point
     */
    public RowSet fetch(Endpoint ep, Schema schema) {
        RowSet rs = new RowSet(schema);
        Field[] fields = Field.from(schema);
        FlightEndpoint fep = new FlightEndpoint(new Ticket(ep.getTicket()), Arrays.stream(ep.getURIs()).map(Location::new).toArray(Location[]::new));
        try {
            try(FlightStream stream = this._client.getStream(fep.getTicket(), this._bearerToken)) {
                VectorSchemaRoot root = stream.getRoot();
                while (stream.next()) {
                    FieldVector[] fs = root.getFieldVectors().stream().map(fv -> FieldVector.fromArrow(fv, Field.find(fields, fv.getName()), root.getRowCount())).toArray(FieldVector[]::new);
                    for (int i = 0; i < root.getRowCount(); i++) {
                        RowSet.Row row = new RowSet.Row();
                        for (FieldVector f : fs) {
                            row.add((f.getValues())[i]);
                        }
                        rs.add(row);
                    }
                }
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        return rs;
    }

    /**
     * Fetch rows from all end-points
     * @param qEndpoints - the query end-points
     * @return - all rows from the end-points
     */
    public RowSet fetch(QueryEndpoints qEndpoints) {
        RowSet rs = new RowSet(qEndpoints.getSchema());
        Arrays.stream(qEndpoints.getEndpoints()).forEach(ep -> rs.add(fetch(ep, qEndpoints.getSchema())));
        return rs;
    }

    /**
     * Execute a literal SQL statement
     * @param stmt - the literal sql-statement
     */
    public long execute(String stmt) {
        FlightInfo fi = this._sqlClient.execute(stmt, this._bearerToken);
        long count = 0;
        for (FlightEndpoint endpoint: fi.getEndpoints()) {
            try {
                try(FlightStream stream = this._client.getStream(endpoint.getTicket(), this._bearerToken)) {
                    while (stream.next()) {
                        VectorSchemaRoot root = stream.getRoot();
                        count += root.getRowCount();
                    }
                }
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }
        return count;
    }

    /**
     * Truncate the target table
     * @param table - the name of the table
     */
    public void truncate(String table) {
        this.execute(String.format("truncate table %s", table));
    }

    /**
     * Get a prepared-statement for a query
     * @param query - the query being used for update
     * @return - the prepared sql-statement for down-stream operations
     */
    public FlightSqlClient.PreparedStatement getPreparedStatement(String query) {
        return this._sqlClient.prepare(query, this._bearerToken);
    }

    /**
     * Execute a prepared-statement
     * @param preparedStmt - the prepared-statement being executed.
     * @return - the number of rows affected.
     */
    public long executeUpdate(FlightSqlClient.PreparedStatement preparedStmt) {
        return preparedStmt.executeUpdate(this._bearerToken);
    }

    /**
     * Close the connection
     */
    @Override
    public void close() {
        try {
            synchronized (Client._clients) {
                Client._clients.remove(this._connectionString);
            }
            this._client.close();

            this._allocator.getChildAllocators().forEach(BufferAllocator::close);
            AutoCloseables.close(this._allocator);
        } catch (Exception ex) {
            LoggerFactory.getLogger(this.getClass()).warn(ex.getMessage() + Arrays.toString(ex.getStackTrace()));
        }
    }

    /**
     * Get a client object
     * @param config - the connection configuration for establishing connections to remote flight service
     * @return - the client object
     */
    public static synchronized Client getOrCreate(Configuration config) {
        String cs = config.getConnectionString();
        if (!Client._clients.containsKey(cs)) {
            final BufferAllocator allocator = new RootAllocator(Integer.MAX_VALUE);
            final FlightClient client = Client.create(config, allocator);

            final CallHeaders callHeaders = new FlightCallHeaders();
            if (config.getDefaultSchema() != null && config.getDefaultSchema().length() > 0) {
                callHeaders.insert("SCHEMA", config.getDefaultSchema());
            }
            if (config.getRoutingTag() != null && config.getRoutingTag().length() > 0) {
                callHeaders.insert("ROUTING_TAG", config.getRoutingTag());
            }
            if (config.getRoutingQueue() != null && config.getRoutingQueue().length() > 0) {
                callHeaders.insert("ROUTING_QUEUE", config.getRoutingQueue());
            }
            final HeaderCallOption clientProperties = (callHeaders.keys().size() > 0) ? new HeaderCallOption(callHeaders) : null;

            Client._clients.put(cs, new Client(client, authenticate(client, config.getUser(), config.getPassword(), config.getBearerToken(), clientProperties), cs, allocator));
        }
        return Client._clients.get(cs);
    }

    //Create a client object with the service configuration
    private static FlightClient create(Configuration config, BufferAllocator allocator) {
        FlightClient.Builder builder = FlightClient.builder().allocator(allocator);
        if (config.getTlsEnabled()) {
            if (config.getTruststoreJks() == null || config.getTruststoreJks().isEmpty()) {
                builder.location(Location.forGrpcTls(config.getFlightHost(), config.getFlightPort())).useTls().verifyServer(config.verifyServer());
            } else {
                builder.location(Location.forGrpcTls(config.getFlightHost(), config.getFlightPort())).useTls().trustedCertificates(new ByteArrayInputStream(config.getCertificateBytes()));
            }
        } else {
            builder.location(Location.forGrpcInsecure(config.getFlightHost(), config.getFlightPort()));
        }
        return (config.getPassword() != null && config.getPassword().length() > 0) ? builder.intercept(Client._factory).build() : builder.build();
    }
    //Authenticate with user & password to obtain the credential token-ticket
    private static CredentialCallOption authenticate(FlightClient client, String user, String password, String bearerToken, HeaderCallOption clientProperties) {
        final java.util.List<CallOption> callOptions = new java.util.ArrayList<>();
        callOptions.add(clientProperties);

        boolean usePassword = (password != null && password.length() > 0);
        callOptions.add(new CredentialCallOption(usePassword ? new BasicAuthCredentialWriter(user, password) : new BearerCredentialWriter(bearerToken)));
        client.handshake(callOptions.toArray(new CallOption[0]));
        return usePassword ? Client._factory.getCredentialCallOption() : (CredentialCallOption)callOptions.get(0);
    }
}
