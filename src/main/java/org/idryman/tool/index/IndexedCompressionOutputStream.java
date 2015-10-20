package org.idryman.tool.index;

import java.io.DataOutputStream;
import java.io.FilterOutputStream;
import java.io.OutputStream;

// this should be an abstract class
public class IndexedCompressionOutputStream extends FilterOutputStream{
  protected DataOutputStream dos;
  //private BufferedOutputStream bos;

/*
 * Initialize with an output stream
 * 
 * dos might points to another stream
 */
  public IndexedCompressionOutputStream(OutputStream out) {
    super(new DataOutputStream(out));
    dos = (DataOutputStream) super.out;
    // I should just extends DOS?
    
    
  }
  
  /*
   * write object to dos
   * dos might be wrapped by multiple buffer, for example...
   * dos(indexedCompression(deflate(dos)))
   * 
   * This class should support recursive buffer, like the one above
   */
  
  /*
   * I can get a Writable object, and write to dos
   */
}
