package org.idryman.tool.index;

import java.io.FilterOutputStream;
import java.io.IOException;
import java.io.OutputStream;

import org.apache.log4j.Logger;

public class Pfor384DictVarintOutputStream extends FilterOutputStream implements LongOutputStream{
  private final static Logger LOG = Logger.getLogger(Pfor384DictVarintOutputStream.class);
  private final static int [] schemes = {4,6,8,12,16,24,32,48,64};
  private final long [] buffer;
  private final byte [] byteBuf = new byte[48];
  private int posIn = 0, posOut=0;
  private int penalty, patchOffset, varints;
  private DictVarintOutputStream dvos;
  
  public Pfor384DictVarintOutputStream(OutputStream out) {
    this(out, 8192);
  }
  
  public Pfor384DictVarintOutputStream(OutputStream out, int size) {
    super(out);
    buffer = new long[size];
  }

  @Override
  public void writeLong(long number) throws IOException {
    if (posIn >= buffer.length) {
      adjustBuffer();
    }
    buffer[posIn++] = number;
    if (posIn-posOut >= 96) {
      flushGroupInts();
    }
  }
  
  private void adjustBuffer() {
    LOG.info("adjust buffer");
    for(int i=posOut, j=0; i<posIn; i++, j++) {
      buffer[j] = buffer[i];
    }
    posIn = posIn-posOut;
    posOut=0;
  }
  
  private void flushGroupInts() throws IOException {
    final int [] paramRef = new int [3]; // penalty, patch_offset, varints 
    
    final int decision = 0;//makeDecision(paramRef);
    //writeWithScheme(buffer, decision);
    LOG.assertLog(decision != -1, "decision should not be -1");
  }

  @Override
  public int estimateBytes(long[] numbers) {
    // TODO Auto-generated method stub
    return 0;
  }

}
