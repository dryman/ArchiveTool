package org.idryman.tool.fs;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileChecksum;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.util.Progressable;

import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;

/**
 * 
 * Valid uri:
 * 1. har2:/archive-path/sub-directories
 * 2. har2:/archive-path.har2/sub-directories
 * 3. har2://ClusterX/archive-path/sub-directories
 *    -Dfs.har2.alias.ClusterX.scheme=hdfs (default)
 *    -Dfs.har2.alias.ClusterX.authority=user:password@namenode:port
 * You can use glob on sub-directories, but can not use on archive-path.
 * archive doesn't need .har2 postfix.
 */
public class Har2FileSystem extends FileSystem {
  private FileSystem fs;
  private Path archivePath;              // qualified archivePath (scheme is har2)
  private Path underlyingArchivePath;    // qualified underlying archivePath
  private Map<Path, Har2FileStatus> fileIndex;
  private MapWritable dirIndex;
  
  /**
   * Constructor for Har2 FileSystem
   *
   */
  public Har2FileSystem() {
  }
  
  /** 
   * TODO Not documented well yet
   * Called after a new FileSystem instance is constructed.
   * @param name a uri whose authority section names the host, port, etc.
   *   for this FileSystem
   * @param conf the configuration
   */
  @Override
  public void initialize(URI uri, Configuration conf) throws IOException {
    super.initialize(uri, conf);
    /*
     * TODO in future version, maybe we can make Har2FileSystem able to stay in FileSystem Cache.
     */
    Preconditions.checkArgument(conf.getBoolean("fs.har2.impl.disable.cache", false),
        "fs.har2.impl.disable.cache need to set to true, else Har2FileSystem wouldn't work properly");
    
    // Setup underlying file system
    String authority = uri.getAuthority();
    if (authority!=null) {
      String underlyingScheme    = conf.get(String.format("fs.har2.alias.%s.scheme", authority));
      String underlyingAuthority = conf.get(String.format("fs.har2.alias.%s.authority", authority));
      if (underlyingScheme!=null && underlyingAuthority==null) {
        if (underlyingScheme.equals(FileSystem.getDefaultUri(conf).getScheme())) {
          fs = FileSystem.get(conf);
        }
      } else {
        URI nonDefaultURI;
        try {
          nonDefaultURI = new URI(underlyingScheme, underlyingAuthority, "", "", "");
          fs = FileSystem.get(nonDefaultURI, conf);
        } catch (URISyntaxException e) {
          throw new IOException(e);
        }
        
      }
    } else {
      fs = FileSystem.get(conf);
    }
    
    /*
     * The archive directory can be either a path ends with .har2,
     * or a directory that contains an empty file _HAR2_
     */
    Path underlyingRoot = new Path(fs.getScheme(),fs.getUri().getAuthority(), "/");
    underlyingArchivePath = new Path(underlyingRoot, uri.getPath());
    while(!underlyingRoot.equals(underlyingArchivePath)) {
      if (fs.exists(new Path(underlyingArchivePath, "_HAR2_")) ||
          underlyingArchivePath.toString().endsWith(".har2")) {
        break;
      }
      underlyingArchivePath = underlyingArchivePath.getParent();
    }
    if (underlyingRoot.equals(underlyingArchivePath) && 
        !fs.exists(new Path(underlyingArchivePath, "_HAR2_"))) {
      throw new IOException("Invalid Har2 URI: "+uri);
    }
    
    archivePath = new Path("har2", authority, 
        Path.getPathWithoutSchemeAndAuthority(underlyingArchivePath).toString());
    
    //uri = har2_path.toUri();
    LOG.debug("underlyingArchivePath is " + underlyingArchivePath);
    LOG.debug("archivePath is: " + archivePath);
    
    // In real application, there would be multiple fileIndexes.
    fileIndex = Maps.newHashMap();

    
    // TODO read dir indexes
    /*
     * there should be a file that contains all dir -> files in dir binding
     * however, the index files would also contain the dirs, but only store the file status 
     */
    // TODO correct user permissions in status
    
    for (FileStatus stat : fs.globStatus(new Path(underlyingArchivePath,"index-*"))) {
      FSDataInputStream fis = fs.open(stat.getPath());
      while (fis.available() > 0) {
        Har2FileStatus h2Status = new Har2FileStatus();
        h2Status.readFields(fis);
        h2Status.makeQualifiedHar2Status(archivePath);
        fileIndex.put(h2Status.getPath(), h2Status);
        LOG.debug("Path loaded: " + h2Status.getPath());
      }
      fis.close();
    }
    
    dirIndex = new MapWritable();
    FSDataInputStream fis = fs.open(new Path(underlyingArchivePath, "directoryMap"));
    dirIndex.readFields(fis);
    fis.close();
    
    
    for (Entry<Writable, Writable> entry : dirIndex.entrySet()) {
      LOG.debug("dirMap key: " + entry.getKey().toString());
    }

    //Thread.dumpStack();
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
  
  /**
   * Returns the uri of this filesystem.
   * The uri is of the form 
   * 1. har2:/archive-path/
   * 2. har2:/archive-path.har2/
   * 3. har2://ClusterX/archive-path/
   *    -Dfs.har2.alias.ClusterX.scheme=hdfs (default)
   *    -Dfs.har2.alias.ClusterX.authority=user:password@namenode:port
   * @return uri URI that identifies the path to archive.
   */
  @Override
  public URI getUri() {
    return archivePath.toUri();
  }
  
  /** 
   * Check that a Path belongs to this FileSystem.
   * @param path to check
   */
  @Override
  protected void checkPath(Path path) {
    LOG.debug("using my checkpath");
    // TODO check if scheme and authority matches ours
    // TODO check if it contains our archive path
    // throw IAE if above doesn't match
    // do nothing for now
  }
  
  @Override
  public FileStatus[] listStatus(Path path) throws FileNotFoundException,
      IOException {
    LOG.debug("using my listStatus");
    Text key = new Text(Har2FileStatus.relativizePath(archivePath, path).toString());
    LOG.debug("querying key is: " + key);
    return ((Har2ArrayWritable) dirIndex.get(key)).toHar2FileStatusArray();
  }
  
  @Override
  public BlockLocation[] getFileBlockLocations(FileStatus file, 
      long start, long len) throws IOException {
    return null;
  }
  
  @Override
  public BlockLocation[] getFileBlockLocations(Path p, 
      long start, long len) throws IOException {
    return null;
  }
  
  
  /**
   * Return the top level archive path.
   * @return archivePath top level archive path with scheme har2://
   */
  @Override
  public Path getWorkingDirectory() {
    return archivePath;
  }

  /**
   * Return the top level archive path.
   * @return archivePath top level archive path with scheme har2://
   */
  @Override
  public Path getInitialWorkingDirectory() {
    return getWorkingDirectory();
  }
  
  
  @Override
  public FileStatus getFileStatus(Path p) throws IOException {
    LOG.debug("geting file status for path: " + p);
    // TODO throws FileNotFoundException if not found
    return fileIndex.get(p);
  }
  
  
  /**
   * Get the checksum of a file, from the beginning of the file till the
   * specific length.
   * @param f The file path
   * @param length The length of the file range for checksum calculation
   * @return The file checksum.
   */
  public FileChecksum getFileChecksum(Path f, final long length)
      throws IOException {
    return null;
    // TODO since xz gives us check sum automatically, we can use it?
  }
  
  @Override
  public FSDataInputStream open (Path f, int bufferSize) throws IOException {
    LOG.debug("Opening path: " + f);
    Har2FileStatus status = fileIndex.get(f);

    int block_num = status.getXZBlockId();
    if (block_num < 0) {
      return new FSDataInputStream(new Har2InputStream.EmtpyInputStream());
    }
    LOG.debug("Partition is: " + status.getPartition());
    return new FSDataInputStream(new Har2InputStream(new Path(underlyingArchivePath, status.getPartition()), block_num));
  }
  
  @Override
  public void close() throws IOException {
    fs.close();
  }
  
  /*
   ***********************************
   *      Unsupported operations     *
   ***********************************
   */
  
  @Override
  public FSDataOutputStream create(Path f, FsPermission permission,
      boolean overwrite, int bufferSize, short replication, long blockSize,
      Progressable progress) throws IOException {
    throw new UnsupportedOperationException("Not implemented by the " + 
      getClass().getSimpleName() + " FileSystem implementation");
  }
  
  
  @Override
  public boolean mkdirs(Path f, FsPermission permission) throws IOException {
    throw new UnsupportedOperationException("Not implemented by the " + 
        getClass().getSimpleName() + " FileSystem implementation");
  }
  

  @Override
  public FSDataOutputStream append(Path f, int bufferSize, Progressable progress)
      throws IOException {
    throw new UnsupportedOperationException("Not implemented by the " + 
        getClass().getSimpleName() + " FileSystem implementation");
  }


  @Override
  public boolean rename(Path src, Path dst) throws IOException {
    throw new UnsupportedOperationException("Not implemented by the " + 
        getClass().getSimpleName() + " FileSystem implementation");
  }


  @Override
  public boolean delete(Path f, boolean recursive) throws IOException {
    throw new UnsupportedOperationException("Not implemented by the " + 
        getClass().getSimpleName() + " FileSystem implementation");
  }

  @Override
  public void setWorkingDirectory(Path new_dir) {
    throw new UnsupportedOperationException("Not implemented by the " + 
        getClass().getSimpleName() + " FileSystem implementation");
  }


}
