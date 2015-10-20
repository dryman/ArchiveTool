package org.idryman.tool.index;

import java.io.BufferedOutputStream;
import java.io.IOException;
import java.io.OutputStream;

public class VarIntOutputStream extends BufferedOutputStream{
  private final static int [] mask_shift = {28,21,14,7};
  private byte varint_buf[] = new byte[5];
  

  public VarIntOutputStream(OutputStream out) {
    super(out);
  }
  
  public VarIntOutputStream(OutputStream out, int size) {
    super(out, size);
  }
  
  /**
   * Convert an integer to varint and write to stream in big endian order.
   * (most significant byte first).
   * @param i
   * @throws IOException
   */
  public synchronized void writeInt(int i) throws IOException {
    //System.out.println("------");
    int masked, pos=0;
    boolean seen = false;
    for (int shift : mask_shift) {
      masked = i & (0x7F << shift);
      if (seen || masked != 0) {
        varint_buf[pos++] = (byte) ((masked >>>= shift) | 0x80);
        seen = true;
      }
    }
    int val = i & 0x7F;
    //printHex(val);
    varint_buf[pos++] = (byte) val;
    super.write(varint_buf, 0, pos);
    //System.out.println("------");
  }
  
  /*
   * TODO make other methods unavailable
   */
  
  public static void printHex(int i) {
    System.out.println(String.format("0x%2s", Integer.toHexString(i)));
  }
}
