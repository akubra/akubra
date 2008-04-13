package org.fedoracommons.akubra;

import java.io.InputStream;

public interface Content {
    
    long getSize();
    
    InputStream getInputStream();
    
    // Q from Pradeep: what about adding an SHA-1 digest as an 
    // additional content metadata that gets stored and used.
    // Maybe another capability of the store?

}
