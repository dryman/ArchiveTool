package org.idryman.tool.index;

import java.io.FilterInputStream;
import java.io.IOException;
import java.io.InputStream;

public class UnaryCodeInputStream extends FilterInputStream{
  private int pz=0, ps=0;
  private byte b;
  
  protected UnaryCodeInputStream(InputStream in) {
    super(in);
  }
  
  public synchronized int readInts(int[]out, int offset, int len) throws IOException {
    int iter = offset, end = offset+len, tmp;
    while (iter < end) {
      tmp = read();
      if (tmp==-1) {
        return iter == offset ? -1 : iter - offset;
      }
      b = (byte) tmp;
      
      while(true) {
        if (b==0) {
          pz+=8-ps;
          ps = 0;
          break;
        }
        int cnt = ntz(b);
        out[iter++] = cnt - ps + pz;
        ps = cnt+1;
        pz = 0;
        b = (byte) (b & (b-1));
        
      }
    }
    
    return 0;
  }
  
  private static int ntz(byte x) {
    if (x==0) return 8;
    int n = 1;
    if ((x&0x0f)==0) { n+=4; x>>>=4;}
    if ((x&0x03)==0) { n+=2; x>>>=2;}
    return n-(x&1);
  }
  
  public static void main(String[] args) throws IOException {
    System.out.println(ntz((byte)0x20));
  }
}
