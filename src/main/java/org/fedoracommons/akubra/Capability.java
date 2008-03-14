package org.fedoracommons.akubra;

public interface Capability {
    
    boolean isCapable();
   
    /**
     * Tells whether this capability is optional or not.
     * 
     * Note that an implementation can start out having this capability
     * as optional, but once some blobs are stored or based on other
     * runtime situations, it is possible that this capability effectively
     * becomes non-optional (as reflected in the two exceptions declared in the
     * setEnabled() call).
     * 
     * @return whether it's optional or not.
     */
    boolean isOptional();
    
    boolean isEnabled();
    
    void setEnabled(boolean enabled)
            throws IllegalStateException, UnsupportedOperationException;

}
