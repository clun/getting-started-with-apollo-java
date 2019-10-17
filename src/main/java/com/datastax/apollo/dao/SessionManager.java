package com.datastax.apollo.dao;

import com.datastax.oss.driver.api.core.CqlSession;

/**
 * {@link CqlSession} will be created for each call and drop after.
 */
public class SessionManager {
    
    /** Singleton Pattern. */
    private static SessionManager _instance = null;
    
    /** Connectivity Attributes. */
    private String userName;
    private String password;
    private String keySpace;
    private String secureConnectionBundlePath;
    
    /** Status and working session. */
    private boolean initialized = false;
    private CqlSession cqlSession;
    
    /**
     * Utility Method to initialized parameters.
     *
     * @return
     *      singletong of the session Manager
     */
    public static synchronized SessionManager getInstance() {
        if (null == _instance) {
            _instance = new SessionManager();
        }
        return _instance;
    }
    
    /**
     * Initialize parameters.
     *
     * @param userName
     *      current username
     * @param password
     *      current password
     * @param secureConnectionBundlePath
     *      zip bundle path on disl
     * @param keyspace
     *      current keyspace
     */
    public void init(String userName, String password, String secureConnectionBundlePath, String keyspace) {
        this.userName                   = userName;
        this.password                   = password;
        this.secureConnectionBundlePath = secureConnectionBundlePath;
        this.keySpace                   = keyspace;
        this.initialized                = true;
    }
    
    /**
     * Getter accessor for attribute 'cqlSession'.
     *
     * @return
     *       current value of 'cqlSession'
     */
    public CqlSession getCqlSession() {
        if (!isInitialized()) {
            throw new IllegalArgumentException("Please initialize the connection parameters first with init(...)");
        }
        if (null == cqlSession) {
            cqlSession = CqlSession.builder().withCloudSecureConnectBundle(getSecureConnectionBundlePath())
                    .withAuthCredentials(getUserName(),getPassword())
                    .withKeyspace(getKeySpace())
                    .build();
        }
        return cqlSession;
    }
    
    public void close() {
        if (isInitialized() && null != cqlSession) {
            cqlSession.close();
        }
    }

    /**
     * Getter accessor for attribute 'userName'.
     *
     * @return
     *       current value of 'userName'
     */
    public String getUserName() {
        return userName;
    }

    /**
     * Getter accessor for attribute 'password'.
     *
     * @return
     *       current value of 'password'
     */
    public String getPassword() {
        return password;
    }

    /**
     * Getter accessor for attribute 'secureConnectionBundlePath'.
     *
     * @return
     *       current value of 'secureConnectionBundlePath'
     */
    public String getSecureConnectionBundlePath() {
        return secureConnectionBundlePath;
    }

    /**
     * Getter accessor for attribute 'keySpace'.
     *
     * @return
     *       current value of 'keySpace'
     */
    public String getKeySpace() {
        return keySpace;
    }

    /**
     * Getter accessor for attribute 'initialized'.
     *
     * @return
     *       current value of 'initialized'
     */
    public boolean isInitialized() {
        return initialized;
    }    
    
}
