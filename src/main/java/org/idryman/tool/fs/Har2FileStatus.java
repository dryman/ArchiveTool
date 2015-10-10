package org.idryman.tool.fs;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;

import com.google.common.base.Preconditions;


// The problem with writable object is it is hard to do versioning..
// maybe I should use Arvo?
public class Har2FileStatus extends FileStatus {
  private static final short VERSION = 1;
  private short   version;
  private Text    jobUUID;
  private Text    partition;
  private long    offset;
  private transient boolean initialized;
  private static final Path rootPath = new Path("..");
  
  // For ReflectionUtil use
  public Har2FileStatus () {
    super();
    version = VERSION;
    jobUUID = new Text();
    partition = new Text();
  }
  
  public Har2FileStatus (FileStatus f, String jobUUID, Path parent) throws IOException {
    super(f);
    this.partition = new Text();
    this.jobUUID   = new Text(jobUUID);
    setPath(relativizePath(parent, getPath()));
  }
  
  public static Path relativizePath(Path parent, Path child) {
    Path p = Path.getPathWithoutSchemeAndAuthority(parent);
    Path c = Path.getPathWithoutSchemeAndAuthority(child);
    return new Path(p.toUri().relativize(c.toUri()));
  }
  
  @Override
  public void write(DataOutput out) throws IOException {
    Preconditions.checkState(!getPath().isAbsolute(), getPath()
        + " should convert to relative before write to disk. "
        + "Did you forget to call makeRelativeHar2Status(Path parent)?");
    super.write(out);
    out.writeShort(VERSION);
    jobUUID.write(out);
    partition.write(out);
    out.writeLong(offset);
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    super.readFields(in);
    version = in.readShort();
    // No version logic for now
    jobUUID.readFields(in);
    partition.readFields(in);
    offset = in.readLong();
    initialized = false;
  }
  
  public int getVersion() {
    return version;
  }
  
  public Path getRelativePartitionPath() {
    return new Path(jobUUID.toString(), partition.toString());
  }
  
  public int getXZBlockOffset() {
    return (int)offset;
  }
  
  public long getOffset() {
    return offset;
  }
  
  /**
   * TODO document normal case and empty file case
   * @param partition Partition file name
   * @param offset Offset of the archived file in the partition. 
   *   If compressed by xz, it's the random access block offset in
   *   xz compressed partition.
   */
  public void setPartitionAndOffset(String partition, long offset) {
    this.partition.set(partition);
    this.offset = offset;
  }

  /**
   * Lazily convert a relativePath to har2:/archiveParent.har2/relativePath
   * @param archivePath A parent path with the scheme har2
   * TODO lazily loading may not be a good idea here..
   */
  public void makeQualifiedHar2Status(Path archivePath) {
    if (initialized) return;
    Preconditions.checkArgument("har2".equals(archivePath.toUri().getScheme()), "archivePath should have scheme as har2");
    if (getPath().equals(rootPath)) {
      setPath(archivePath);
    } else {
      setPath(new Path(archivePath, getPath()));
    }
    initialized = true;
  }
  
  /*
   * TODO toString()
   */
}
