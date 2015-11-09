package org.idryman.tool.index;

import java.io.FilterOutputStream;
import java.io.IOException;
import java.io.OutputStream;

public class Pfor384DictVarintOutputStream extends FilterOutputStream implements LongOutputStream{

  public Pfor384DictVarintOutputStream(OutputStream out) {
    super(out);
  }

  @Override
  public void writeLong(long number) throws IOException {
    // TODO Auto-generated method stub
    
  }

  @Override
  public int estimateBytes(long[] numbers) {
    // TODO Auto-generated method stub
    return 0;
  }

}
