package org.fedoracommons.akubra;

public interface BlobStore {
    
  BlobStore getBackingStore();
  
  void setBackingStore(BlobStore store)
          throws UnsupportedOperationException, IllegalStateException;
  
  BlobStoreConnection openConnection();

}
