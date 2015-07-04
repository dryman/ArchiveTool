package org.idryman.tool.fs;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FilterFileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;

import com.google.common.base.Preconditions;

public class Har2FileSystem extends FilterFileSystem {
  private URI uri;
  private Path archivePath;
  private String harAuth;
  private MapWritable fileIndex;
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
    URI underlyingURI = decodeHar2URI(name, conf);
    Path har2_path = new Path(name.getScheme(), name.getAuthority(), name.getPath());
    
    if (fs == null) {
      fs = FileSystem.get(conf); // FIXME use default FS first
    }
    Path cur = new Path(".");
    cur = fs.getFileStatus(cur).getPath();
    //fs.makeQualified(cur);
    LOG.debug("cur is qualified: " + cur);
    
    
    uri = har2_path.toUri();
    archivePath = new Path(name.getScheme(), name.getAuthority(), Path.getPathWithoutSchemeAndAuthority(cur).toString());
    LOG.debug("archivePath is: " + archivePath);
    harAuth = getHarAuth(underlyingURI);
    
    // In real application, there would be multiple fileIndexes.
    fileIndex = new MapWritable();
    FSDataInputStream fis = fs.open(new Path(cur, "_index"));
    fileIndex.readFields(fis);
    fis.close();
    
    
    super.initialize(name, conf);
    LOG.debug("Initialized");
    //Thread.dumpStack();
  }
  
  /**
   * return the top level archive.
   */
  @Override
  public Path getWorkingDirectory() {
    return new Path(uri.toString());
  }

  @Override
  public Path getInitialWorkingDirectory() {
    return getWorkingDirectory();
  }

  /* this makes a path qualified in the har filesystem
   * (non-Javadoc)
   * @see org.apache.hadoop.fs.FilterFileSystem#makeQualified(
   * org.apache.hadoop.fs.Path)
   */
  @Override
  public Path makeQualified(Path path) {
    // make sure that we just get the 
    // path component 
    LOG.debug("my makeQualified");
    Path fsPath = path;
    if (!path.isAbsolute()) {
      fsPath = new Path(archivePath, path);
    }

    URI tmpURI = fsPath.toUri();
    //change this to Har uri 
    return new Path(uri.getScheme(), harAuth, tmpURI.getPath());
  }
  
  private String getHarAuth(URI underLyingUri) {
    String auth = underLyingUri.getScheme() + "-";
    if (underLyingUri.getHost() != null) {
      if (underLyingUri.getUserInfo() != null) {
        auth += underLyingUri.getUserInfo();
        auth += "@";
      }
      auth += underLyingUri.getHost();
      if (underLyingUri.getPort() != -1) {
        auth += ":";
        auth +=  underLyingUri.getPort();
      }
    }
    else {
      auth += ":";
    }
    return auth;
  }
  
  /**
   * Returns the uri of this filesystem.
   * The uri is of the form 
   * har://underlyingfsschema-host:port/pathintheunderlyingfs
   */
  @Override
  public URI getUri() {
    return this.uri;
  }
  
  private static String getRelativePathString(Path parent, Path child) {
    Path p = Path.getPathWithoutSchemeAndAuthority(parent);
    Path c = Path.getPathWithoutSchemeAndAuthority(child);
    return p.toUri().relativize(c.toUri()).getPath();
  }
  
  @Override
  public FileStatus getFileStatus(Path p) throws IOException {
    
    String relativePathString = getRelativePathString(archivePath, p);
    Har2FileStatus status = (Har2FileStatus) fileIndex.get(new Text(relativePathString));
    status.makeQualifiedHar2Status(archivePath);
    
    return status;
  }
  
  private URI decodeHar2URI(URI raw_uri, Configuration conf) throws IOException {
    String authority = raw_uri.getAuthority();
    if (authority == null) {
      return FileSystem.getDefaultUri(conf);
    }
    int i = authority.indexOf('-');
    if (i < 0) {
      throw new IOException("URI: " + raw_uri
          + " is an invalid Har URI since '-' not found."
          + "  Expecting har://<scheme>-<host>/<path>.");
    }
    
    if (raw_uri.getQuery() != null) {
      // query component not allowed
      throw new IOException("query component in Path not supported  " + raw_uri);
    }
    
    URI tmp;
    try {
      // convert <scheme>-<host> to <scheme>://<host>
      URI baseUri = new URI(authority.replaceFirst("-", "://"));
 
      tmp = new URI(baseUri.getScheme(), baseUri.getAuthority(),
          raw_uri.getPath(), raw_uri.getQuery(), raw_uri.getFragment());
    } catch (URISyntaxException e) {
      throw new IOException("URI: " + raw_uri
          + " is an invalid Har URI. Expecting har://<scheme>-<host>/<path>.");
    }
    return tmp;
  }
  
  /*
   * find the parent path that is the 
   * archive path in the path. The last
   * path segment that ends with .har is 
   * the path that will be returned.
   */
  private Path archivePath(Path p) {
    // TODO might not be the best way to use har
    // 
    Path retPath = null;
    Path tmp = p;
    for (int i=0; i< p.depth(); i++) {
      if (tmp.toString().endsWith(".har2")) {
        retPath = tmp;
        break;
      }
      tmp = tmp.getParent();
    }
    return retPath;
  }
  
  @Override
  public FSDataInputStream open (Path f, int bufferSize) throws IOException {
    LOG.debug("Using my open");
    
    String relativePathString = getRelativePathString(archivePath, f);
    Har2FileStatus status = (Har2FileStatus) fileIndex.get(new Text(relativePathString));
    status.makeQualifiedHar2Status(archivePath);
    
    
    int block_num = status.getXZBlockId();
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
  public Path resolvePath(final Path p) throws IOException {
    LOG.debug("using my resolvePath");
    return p;
  }
}
