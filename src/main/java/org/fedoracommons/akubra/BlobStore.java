package org.fedoracommons.akubra;

public interface BlobStore {
    
    Capability[] getDeclaredCapabilities();

    /**
     * Gets a combination of this store's declared capabilities and those
     * of the backing store.
     * 
     * @return the capabilities.
     */
    Capability[] getCapabilities();
    
    BlobStore getBackingStore();
    
    void setBackingStore(BlobStore store)
            throws UnsupportedOperationException, IllegalStateException;
    
    BlobStoreConnection openConnection();

}
