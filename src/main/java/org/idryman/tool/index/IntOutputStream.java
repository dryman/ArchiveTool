package org.idryman.tool.index;

import java.io.IOException;

public interface IntOutputStream {
  public void writeInt(int number) throws IOException;
  public int estimateBytes(int [] numbers);
}
