package org.idryman.tool.index;

import java.io.IOException;

public interface LongOutputStream {
  
  public void writeLong(long number) throws IOException;
  
  public int estimateBytes(long [] numbers);
}
