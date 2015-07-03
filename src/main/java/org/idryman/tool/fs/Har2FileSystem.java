package org.idryman.tool.fs;

import java.io.File;
import java.io.IOException;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FilterFileSystem;
import org.apache.hadoop.fs.Path;
import org.tukaani.xz.SeekableFileInputStream;
import org.tukaani.xz.SeekableXZInputStream;

import com.google.common.base.Preconditions;

public class Har2FileSystem extends FilterFileSystem {
  private URI uri;
  /**
   * Constructor for Har2 FileSystem
   *
   */
  public Har2FileSystem() {
  }
  
  /**
   * Constructor to create a Har2 FileSystem from
   * another FileSystem
   *
   */
  public Har2FileSystem(FileSystem fs) {
    super(fs);
  }

  /**
   * Return the protocol scheme for the FileSystem.
   *
   * @return <code>har2</code>
   */
  @Override
  public String getScheme() {
    return "har2";
  }
  
  @Override
  public void initialize(URI name, Configuration conf) throws IOException {
    LOG.debug("Initializing");
    if (fs == null) {
      fs = FileSystem.get(conf); // FIXME use default FS first
    }
    super.initialize(name, conf);
    LOG.debug("Initialized");
  }
  

  
  @Override
  public FSDataInputStream open (Path f, int bufferSize) throws IOException {
    LOG.debug("Using my open");
    int block_num = Integer.parseInt(f.getName());
    File in_file = new File("foo.xz");
    Preconditions.checkState(in_file.exists(), "Input file doesn't exist");
    return new FSDataInputStream(new Har2InputStream(block_num));
  }
  
  @Override
  protected void checkPath(Path path) {
    LOG.debug("using my checkpath");
    // do nothing for now
  }
  
  @Override
  public Path makeQualified(Path path) {
    LOG.debug("using my qualified");
    return path;
  }
  
  @Override
  public Path resolvePath(final Path p) throws IOException {
    LOG.debug("using my resolvePath");
    return p;
  }
}
