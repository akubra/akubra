package org.fedoracommons.akubra;

public interface BlobStoreConnection {
    
    public enum TxnIsolation {
        
        NONE,
        
        /**
         * Note from Pradeep: This capability cannot be plugged in externally,
         * either the store supports it or not. Any layer that emulates this at
         * the client side will only be able to let clients peek into other
         * concurrent transactions it is aware of and nothing more. So if this
         * emulation is important to an app from a server with multiple clients,
         * then a proxy server needs to be set-up. I think this is beyond the
         * scope of this discussion. It (the emulation of UNCOMMITTED_READS) is
         * a real esoteric feature that we may look into in a later version and
         * I am not sure what the value of this is in a blob context. I am even
         * ok if we were to drop the UNCOMMITTED_READS support altogether.
         */
        UNCOMMITTED_READS,
        COMMITTED_READS,
        REPEATABLE_READS;

    }
    
    Content getBlob(String blobId);
   
    /**
     * Stores a blob with the given id.
     * 
     * If a blob id is specified, but such a blob already exists, its content
     * will be replaced.
     * 
     * @param blobId the blob id, or null.
     * @param content the content to store.
     * @return the blob locator-id.
     */
    String putBlob(String blobId, Content content);
    
    String removeBlob(String blobId);
    
    String getBlobLocator(String blobId);
    
    boolean isAutoCommit();
    
    void setAutoCommitMode(boolean mode);
    
    void rollback();
    
    void commit();
    
    void close();
    
    TxnIsolation getTxnIsolationLevel();
    
    void setTxnIsolationLevel(TxnIsolation isolationLevel);
    
    boolean isReadOnly();
    
    void setReadOnly();
   
    // XA stuff commented out for now.
    // Note from Ronald: XA combination must be done in a particular way --
    // consider XAResource.isSameRM().
    // XAResource[] getXAResources();

}
