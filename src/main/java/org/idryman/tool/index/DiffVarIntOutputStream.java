package org.idryman.tool.index;

import java.io.IOException;
import java.io.OutputStream;

public class DiffVarIntOutputStream extends VarIntOutputStream{
  private int prev;

  public DiffVarIntOutputStream(OutputStream out) {
    super(out);
    prev = 0;
  }

  @Override
  public synchronized void writeInt(int i) throws IOException {
    assert(i>=prev);
    super.writeInt(i-prev);
    prev=i;
  }
}
