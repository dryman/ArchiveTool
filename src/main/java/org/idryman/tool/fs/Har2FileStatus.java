package org.idryman.tool.fs;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.commons.httpclient.URI;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;


// The problem with writable object is it is hard to do versioning..
// maybe I should use Arvo?
public class Har2FileStatus extends FileStatus {
  private int blockId; // if we want to make a file splitable in future, this wouldn't work
  private boolean initialized;
  
  // For ReflectionUtil use
  public Har2FileStatus () {
    super();
  }
  
  private Har2FileStatus(FileStatus f, int blockId) throws IOException {
    super(f);
    this.blockId = blockId;
  }
  
  public static Har2FileStatus newFileStatusWithBlockId(FileStatus f, int blockId) throws IOException {
    return new Har2FileStatus(f, blockId);
  }
  
  public void write(DataOutput out) throws IOException {
    super.write(out);
    out.writeInt(blockId);
  }

  public void readFields(DataInput in) throws IOException {
    super.readFields(in);
    blockId = in.readInt();
    initialized = false;
  }

  public void makeQualifiedHar2Status(Path archivePath) {
    if (initialized) return;
    setPath(new Path(archivePath, getPath()));
    initialized = true;
  }
  
  
}
