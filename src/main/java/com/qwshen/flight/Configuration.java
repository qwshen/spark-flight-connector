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

    //the user and password with which to connect to flight service
    private final String _user;
    private final String _password;

    //the binary content of the certificate
    private byte[] _certBytes;

    /**
     * Construct a Configuration object
     * @param host - the host name of the remote flight service
     * @param port - the port number of the remote flight service
     * @param user - the user account for connecting to remote flight service
     * @param password - the password of the user account
     */
    public Configuration(String host, int port, String user, String password) {
        this(host, port, false, false, user, password);
    }

    /**
     * Construct a Configuration object
     * @param host - the host name of the remote flight service
     * @param port - the port number of the remote flight service
     * @param tlsEnabled - whether the flight service has tls enabled for secure connection
     * @param crtVerify -whether to verify the certificate if remote flight service is tls-enabled.
     * @param user - the user account for connecting to remote flight service
     * @param password - the password of the user account
     */
    public Configuration(String host, int port, Boolean tlsEnabled, Boolean crtVerify, String user, String password) {
        this._fsHost = host;
        this._fsPort = port;
        this._tlsEnabled = tlsEnabled;
        this._crtVerify = crtVerify;

        this._trustStoreJks = null;
        this._trustStorePass = null;
        this._certBytes = null;

        this._user = user;
        this._password = password;
    }

    /**
     * Construct a Configuration object
     * @param host - the host name of the remote flight service
     * @param port - the port number of the remote flight service
     * @param trustStoreJks - the filename of the trust store in jks
     * @param truststorePass - the pass code of the trust store
     * @param user - the user account for connecting to remote flight service
     * @param password - the password of the user account
     */
    public Configuration(String host, int port, String trustStoreJks, String truststorePass, String user, String password) {
        this(host, port, true, true, user, password);

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
     * Retrieve the connection string for connecting to the remote flight service
     * @return - the connection string
     */
    public String getConnectionString() {
        return String.format("%s://%s:%s@%s:%d", this._tlsEnabled ? "https:" : "http", this._user, this._password, this._fsHost, this._fsPort);
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
}
