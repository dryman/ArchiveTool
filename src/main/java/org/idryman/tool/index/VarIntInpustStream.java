package org.idryman.tool.index;

import java.io.BufferedInputStream;
import java.io.IOException;
import java.io.InputStream;

public class VarIntInpustStream extends BufferedInputStream{

  public VarIntInpustStream(InputStream in) {
    super(in);
  }

  public VarIntInpustStream(InputStream in, int size) {
    super(in, size);
  }
  
  public synchronized int readInts(int buffer[], int offset, int len) throws IOException {
    int val, tmp, iter = offset, end = offset+len;
    
    while (iter < end) {
      val = 0;
      do {
        tmp = super.read();
        if (tmp==-1) {
          return iter == offset ? -1 : iter - offset; 
        }
        //VarIntOutputStream.printHex(tmp);
        val = (val << 7) | (tmp & 0x7F); 
      } while((tmp & 0x80) != 0);
      buffer[iter++] = val;
      //System.out.println("---------");
    }
    return iter - offset;
  }
  
  /*
   * TODO make other methods unavailable
   */
}
