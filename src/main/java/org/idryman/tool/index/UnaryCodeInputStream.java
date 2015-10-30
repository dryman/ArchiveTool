package org.idryman.tool.index;

import java.io.FilterInputStream;
import java.io.IOException;
import java.io.InputStream;

public class UnaryCodeInputStream extends FilterInputStream{
  private int pz=0, ps=0;
  private byte b;
  private boolean readByte = true;
  
  protected UnaryCodeInputStream(InputStream in) {
    super(in);
  }
  
  public synchronized int readInt() throws IOException {
    while (true) {
      if (readByte) {
        int tmp = read();
        if (tmp == -1) return -1;
        b = (byte) tmp;
      }
      readByte = false;
   
      if (b==0) {
        pz += 8 - ps;
        ps = 0;
        readByte=true;
      } else {
        int cnt = ntz(b);
        int ret = cnt - ps + pz;
        ps = cnt+1;
        pz = 0;
        b = (byte) (b & (b-1));
        return ret;
      }
    }
  }
  
  private static int ntz(byte x) {
    if (x==0) return 8;
    int n = 1;
    if ((x&0x0f)==0) { n+=4; x>>>=4;}
    if ((x&0x03)==0) { n+=2; x>>>=2;}
    return n-(x&1);
  }
}
