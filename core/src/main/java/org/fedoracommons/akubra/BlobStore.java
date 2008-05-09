package org.fedoracommons.akubra;

import java.util.List;

import javax.transaction.TransactionManager;

public interface BlobStore {
    
  List<BlobStore> getBackingStores();
  
  void setBackingStores(List<BlobStore> stores)
          throws UnsupportedOperationException, IllegalStateException;
  
  BlobStoreConnection openConnection(TransactionManager txmgr);

}
