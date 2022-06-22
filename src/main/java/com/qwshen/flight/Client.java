package com.qwshen.flight;

import org.apache.arrow.flight.*;
import org.apache.arrow.flight.auth2.BasicAuthCredentialWriter;
import org.apache.arrow.flight.auth2.ClientBearerHeaderHandler;
import org.apache.arrow.flight.auth2.ClientIncomingAuthHeaderMiddleware;
import org.apache.arrow.flight.grpc.CredentialCallOption;
import org.apache.arrow.flight.sql.FlightSqlClient;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;
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
    //the buffer
    private static final BufferAllocator _allocator = new RootAllocator(Integer.MAX_VALUE);
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

    /**
     * Construct a Client object
     * @param client - the client object of the flight service
     * @param bearerToken - the credential token
     */
    private Client(FlightClient client, CredentialCallOption bearerToken) {
        this._client = client;
        this._sqlClient = new FlightSqlClient(this._client);
        this._bearerToken = bearerToken;
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
     * Truncate the target table
     * @param table - the name of the table
     */
    public void truncate(String table) {
        this._sqlClient.execute(String.format("truncate table %s", table), this._bearerToken);
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
            this._client.close();
        } catch (InterruptedException ex) {
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
            final FlightClient client = Client.create(config);
            Client._clients.put(cs, new Client(client, authenticate(client, config.getUser(), config.getPassword())));
        }
        return Client._clients.get(cs);
    }

    //Create a client object with the service configuration
    private static FlightClient create(Configuration config) {
        FlightClient.Builder builder = FlightClient.builder().allocator(Client._allocator);
        if (config.getTlsEnabled()) {
            if (config.getTruststoreJks() == null || config.getTruststoreJks().isEmpty()) {
                builder.location(Location.forGrpcTls(config.getFlightHost(), config.getFlightPort())).useTls().verifyServer(config.verifyServer());
            } else {
                builder.location(Location.forGrpcTls(config.getFlightHost(), config.getFlightPort())).useTls().trustedCertificates(new ByteArrayInputStream(config.getCertificateBytes()));
            }
        } else {
            builder.location(Location.forGrpcInsecure(config.getFlightHost(), config.getFlightPort()));
        }
        return builder.intercept(Client._factory).build();
    }
    //Authenticate with user & password to obtain the credential token-ticket
    private static CredentialCallOption authenticate(FlightClient client, String user, String password) {
        final java.util.List<CallOption> callOptions = new java.util.ArrayList<>();
        callOptions.add(new CredentialCallOption(new BasicAuthCredentialWriter(user, password)));
        client.handshake(callOptions.toArray(new CallOption[0]));
        return Client._factory.getCredentialCallOption();
    }
}
