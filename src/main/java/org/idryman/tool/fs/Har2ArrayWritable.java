package org.idryman.tool.fs;

import org.apache.hadoop.io.ArrayWritable;

public final class Har2ArrayWritable extends ArrayWritable {
  
  public Har2ArrayWritable() {
    super(Har2FileStatus.class);
  }
  
  public Har2ArrayWritable(Har2FileStatus[] statuses) {
    super(Har2FileStatus.class, statuses);
  }
  
  public Har2FileStatus [] toHar2FileStatusArray() {
    return (Har2FileStatus []) toArray();
  }
}
