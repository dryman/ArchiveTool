package org.idryman.tool.index;

import java.io.BufferedInputStream;
import java.io.IOException;
import java.io.InputStream;

public class DiffVarIntInputStream extends BufferedInputStream{
  private int val;

  public DiffVarIntInputStream(InputStream in) {
    super(in);
    val = 0;
  }

  public synchronized int readInts(int[] buffer, int offset, int len)
      throws IOException {
    int diff_val, tmp, iter = offset, end = offset+len;
    
    while (iter < end) {
      diff_val = 0;
      do {
        tmp = super.read();
        if (tmp==-1) {
          return iter == offset ? -1 : iter - offset;
        }
        diff_val = (diff_val << 7) | (tmp & 0x7F); 
      } while((tmp & 0x80) != 0);
      val+=diff_val;
      buffer[iter++] = val;
    }
    return iter - offset;
  }
}
