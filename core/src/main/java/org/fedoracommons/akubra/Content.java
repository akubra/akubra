package org.fedoracommons.akubra;

import java.io.InputStream;

public interface Content {

  /**
   * Gets an input stream for reading the content.
   *
   * @return the input stream.
   */
  InputStream getInputStream();

  /**
   * Gets the size of the content, in bytes.
   *
   * @return the size in bytes, or -1 if unknown.
   */
  long getSize();
    
}
