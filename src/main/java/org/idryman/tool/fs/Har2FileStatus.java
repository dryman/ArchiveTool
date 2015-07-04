package org.idryman.tool.fs;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;

import com.google.common.base.Preconditions;


// The problem with writable object is it is hard to do versioning..
// maybe I should use Arvo?
public class Har2FileStatus extends FileStatus {
  private int xzBlockId; // if we want to make a file splitable in future, this wouldn't work
  private transient boolean initialized;
  
  // For ReflectionUtil use
  public Har2FileStatus () {
    super();
  }
  
  /**
   * Create Har2FileStatus from ordinary FileStatus and xz block id.
   * @param f ordinary FileStatus
   * @param blockId xz block id
   * @throws IOException
   */
  public Har2FileStatus(FileStatus f, int blockId) throws IOException {
    super(f);
    this.xzBlockId = blockId;
  }
  
  @Override
  public void write(DataOutput out) throws IOException {
    Preconditions.checkState(!getPath().isAbsolute(), getPath()
        + " should convert to relative before write to disk. "
        + "Did you forget to call makeRelativeHar2Status(Path parent)?");
    super.write(out);
    out.writeInt(xzBlockId);
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    super.readFields(in);
    xzBlockId = in.readInt();
    initialized = false;
  }
  
  public int getXZBlockId() {
    return xzBlockId;
  }
  
  /**
   * Remove the parent part of the original file status before write to disk
   * @param parent
   */
  public void makeRelativeHar2Status(Path parent) {
    Path p = Path.getPathWithoutSchemeAndAuthority(parent);
    Path c = Path.getPathWithoutSchemeAndAuthority(getPath());
    setPath(new Path(p.toUri().relativize(c.toUri())));
  }

  /**
   * Lazily convert a relativePath to har2:/archiveParent.har2/relativePath
   * @param archivePath A parent path with the scheme har2
   */
  public void makeQualifiedHar2Status(Path archivePath) {
    if (initialized) return;
    Preconditions.checkArgument("har2".equals(archivePath.toUri().getScheme()), "archivePath should have scheme as har2");
    setPath(new Path(archivePath, getPath()));
    initialized = true;
  }
}
