package org.fedoracommons.akubra;

import java.net.URI;

public interface BlobStoreConnection {
    
  Content getBlob(URI blobId);
   
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
  URI putBlob(URI blobId, Content content);

  URI removeBlob(URI blobId);
    
  URI getBlobLocator(URI blobId);

  void close();
    
}
