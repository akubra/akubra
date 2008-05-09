package org.fedoracommons.akubra;

public interface BlobStoreConnection {
    
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

  void close();
    
}
