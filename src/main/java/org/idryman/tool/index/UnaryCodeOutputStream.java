package org.idryman.tool.index;

import java.io.FilterOutputStream;
import java.io.IOException;
import java.io.OutputStream;

public class UnaryCodeOutputStream extends FilterOutputStream implements IntOutputStream{
  private int ps;
  private byte b;
  private boolean dirtyByte;
  private boolean closed;
  
  public UnaryCodeOutputStream(OutputStream out) {
    super(out);
  }
  
  @Override
  public int estimateBytes(int[] numbers) {
    int sum = 0, ret;
    for (int n : numbers) {
      sum += n+1;
    }
    ret = sum >>> 3;
    return sum - (ret << 3) == 0 ? ret : ret+1;
  }
  
  @Override
  public synchronized void writeInt(int number) throws IOException {
    if (!closed) {
      number+=ps;
      ps=0;
      while(number >= 8) {
        super.write(b);
        b=0;
        number-=8;
      }
      b |= (byte) (1 << number);
      ps = number + 1;
      
      if (ps==8) {
        super.write(b);
        b=0;
        ps=0;
        dirtyByte = false;
      } else {
        dirtyByte = true;
      }
    }
  }
  
  @Override
  public synchronized void close() throws IOException {
    if (!closed) {
      if (dirtyByte) super.write(b);
      closed=true;
    }
    // do not close super.
  }
  
  @Override
  public synchronized void flush() throws IOException {
    this.close();
  }
  
  @Override
  public void write(int b) throws IOException {
    throw new UnsupportedOperationException();
  }
  @Override
  public void write(byte b[]) throws IOException {
    throw new UnsupportedOperationException();
  }
  @Override
  public void write(byte b[], int off, int len) throws IOException {
    throw new UnsupportedOperationException();
  }

}
