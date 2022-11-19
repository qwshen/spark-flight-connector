package com.qwshen.flight;

import org.bouncycastle.openssl.jcajce.JcaPEMWriter;
import org.slf4j.LoggerFactory;
import java.io.IOException;
import java.io.InputStream;
import java.io.Serializable;
import java.io.StringWriter;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.security.KeyStore;
import java.security.cert.Certificate;
import java.util.Enumeration;

/**
 * Describes the data-structure for connecting to remote flight-service.
 */
public class Configuration implements Serializable {
    //the host name of the flight end-point
    private final String _fsHost;
    //the port # of the flight end-point
    private final int _fsPort;
    //the flight end-point is whether tls enabled
    private final Boolean _tlsEnabled;
    private final Boolean _crtVerify;

    //the trust-store file & pass-code
    private String _trustStoreJks;
    private String _trustStorePass;

    //the user and password/access-token with which to connect to flight service
    private final String _user;
    private final String _password;
    private final String _bearerToken;

    //information to manage work-loads
    private String _defaultSchema = "";
    private String _routingTag = "";
    private String _routingQueue = "";

    //the binary content of the certificate
    private byte[] _certBytes;

    /**
     * Construct a Configuration object
     * @param host - the host name of the remote flight service
     * @param port - the port number of the remote flight service
     * @param user - the user account for connecting to remote flight service
     * @param password - the password of the user account
     * @param bearerToken - the pat or auth2 token
     */
    public Configuration(String host, int port, String user, String password, String bearerToken) {
        this(host, port, false, false, user, password, bearerToken);
    }

    /**
     * Construct a Configuration object
     * @param host - the host name of the remote flight service
     * @param port - the port number of the remote flight service
     * @param tlsEnabled - whether the flight service has tls enabled for secure connection
     * @param crtVerify -whether to verify the certificate if remote flight service is tls-enabled.
     * @param user - the user account for connecting to remote flight service
     * @param password - the password of the user account
     * @param bearerToken - the pat or auth2 token
     */
    public Configuration(String host, int port, Boolean tlsEnabled, Boolean crtVerify, String user, String password, String bearerToken) {
        this._fsHost = host;
        this._fsPort = port;
        this._tlsEnabled = tlsEnabled;
        this._crtVerify = crtVerify;

        this._trustStoreJks = null;
        this._trustStorePass = null;
        this._certBytes = null;

        this._user = user;
        this._password = password;
        this._bearerToken = bearerToken;
    }

    /**
     * Construct a Configuration object
     * @param host - the host name of the remote flight service
     * @param port - the port number of the remote flight service
     * @param trustStoreJks - the filename of the trust store in jks
     * @param truststorePass - the pass code of the trust store
     * @param user - the user account for connecting to remote flight service
     * @param password - the password of the user account
     * @param bearerToken - the pat or auth2 token
     */
    public Configuration(String host, int port, String trustStoreJks, String truststorePass, String user, String password, String bearerToken) {
        this(host, port, true, true, user, password, bearerToken);

        this._trustStoreJks = trustStoreJks;
        this._trustStorePass = truststorePass;

        this._certBytes = Configuration.getCertificateBytes(this._trustStoreJks, this._trustStorePass);
    }

    /**
     * Get the host name of the remote flight service
     * @return - the host name
     */
    public String getFlightHost() {
        return this._fsHost;
    }

    /**
     * Get the port number of the remote flight service
     * @return - the port number
     */
    public int getFlightPort() {
        return this._fsPort;
    }

    /**
     * Get the filename of the truststore
     * @return - the filename
     */
    public String getTruststoreJks() {
        return this._trustStoreJks;
    }

    /**
     * Get the password of the truststore.
     * @return - the password
     */
    public String getTruststorePass() {
        return this._trustStorePass;
    }

    /**
     * Get the flag of whether the remote flight service has tls enabled for secure connections
     * @return - true if the remtoe flight service supports secure connections
     */
    public Boolean getTlsEnabled() {
        return this._tlsEnabled;
    }

    /**
     * Get the flag of whether to skip verifying the remote flight service
     * @return - true to skip the verification
     */
    public Boolean verifyServer() {
        return this._crtVerify;
    }

    /**
     * Get the byte conent of the service certificate
     * @return - the byte content
     */
    public byte[] getCertificateBytes() {
        return this._certBytes;
    }

    /**
     * Get the user account
     * @return - the user account
     */
    public String getUser() {
        return this._user;
    }

    /**
     * Get the password of the user account
     * @return - the password of the user account
     */
    public String getPassword() {
        return this._password;
    }
    /**
     * Get the access-token of the user account
     * @return - the access-token of the user account
     */
    public String getBearerToken() {
        return this._bearerToken;
    }

    /**
     * Retrieve the connection string for connecting to the remote flight service
     * @return - the connection string
     */
    public String getConnectionString() {
        String secret = (this._password != null && this._password.length() > 0) ? this._password : this._bearerToken;
        return String.format("%s://%s:%s@%s:%d", this._tlsEnabled ? "https:" : "http", this._user, secret, this._fsHost, this._fsPort);
    }

    private static byte[] getCertificateBytes(String keyStorePath, String keyStorePassword) {
        try {
            KeyStore keyStore = KeyStore.getInstance(KeyStore.getDefaultType());
            try (InputStream keyStoreStream = Files.newInputStream(Paths.get(keyStorePath))) {
                keyStore.load(keyStoreStream, keyStorePassword.toCharArray());
            }

            Enumeration<String> aliases = keyStore.aliases();
            while (aliases.hasMoreElements()) {
                String alias = aliases.nextElement();
                if (keyStore.isCertificateEntry(alias)) {
                    Certificate certificates = keyStore.getCertificate(alias);
                    return toBytes(certificates);
                }
            }
        } catch (Exception e) {
            LoggerFactory.getLogger(Configuration.class).warn("Cannot load the cert - " + keyStorePath);
        }
        return new byte[0];
    }

    private static byte[] toBytes(Certificate certificate) throws IOException {
        try (
            StringWriter writer = new StringWriter();
            JcaPEMWriter pemWriter = new JcaPEMWriter(writer)
        ) {
            pemWriter.writeObject(certificate);
            pemWriter.flush();
            return writer.toString().getBytes(StandardCharsets.UTF_8);
        }
    }

    /**
     * Get the path of the default schema
     * @return - Default schema path to the dataset that the user wants to query.
     */
    public String getDefaultSchema() {
        return _defaultSchema;
    }

    /**
     * Set the path of default schema
     * @param defaultSchema - Default schema path to the dataset that the user wants to query.
     */
    public void setDefaultSchema(String defaultSchema) {
        this._defaultSchema = defaultSchema;
    }

    /**
     * Get the routing-tag
     * @return - Tag name associated with all queries executed within a Flight session. Used only during authentication.
     */
    public String getRoutingTag() {
        return this._routingTag;
    }

    /**
     * Set the rouging-tag
     * @param routingTag - Tag name associated with all queries executed within a Flight session. Used only during authentication.
     */
    public void setRoutingTag(String routingTag) {
        this._routingTag = routingTag;
    }

    /**
     * Get the routing-queue
     * @return - Name of the workload management queue. Used only during authentication.
     */
    public String getRoutingQueue() {
        return this._routingQueue;
    }

    /**
     * Set the routing-queue
     * @param routingQueue - Name of the workload management queue. Used only during authentication.
     */
    public void setRoutingQueue(String routingQueue) {
        this._routingQueue = routingQueue;
    }
}
