package com.qwshen.flight;

import java.io.Serializable;
import java.net.URI;

/**
 * Describes the data-structure of a flight end-point for connections
 */
public class Endpoint implements Serializable {
    //the URIs of the end-point
    private final URI[] _uris;
    //the ticket for connecting to the end-point
    private final byte[] _ticket;

    /**
     * Construct an end-point
     * @param uris - the URIs of the end-point
     * @param ticket - the ticket for connecting to the end-point
     */
    public Endpoint(URI[] uris, byte[] ticket) {
        this._uris = uris;
        this._ticket = ticket;
    }

    /**
     * Get the URIs of the end-point
     * @return - the URIs of the end-point
     */
    public URI[] getURIs() {
        return this._uris;
    }

    /**
     * Get the ticket of the end-point
     * @return - the ticket of the end-point
     */
    public byte[] getTicket() {
        return this._ticket;
    }
}
