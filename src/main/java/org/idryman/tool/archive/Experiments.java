package org.idryman.tool.archive;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class Experiments {
  
  private Path relPathToRoot(Path fullPath, Path root) {

    Path justRoot = new Path(Path.SEPARATOR);
    if (fullPath.depth() == root.depth()) {
      return justRoot;
    }
    else if (fullPath.depth() > root.depth()) {
      Path retPath = new Path(fullPath.getName());
      Path parent = fullPath.getParent();
      for (int i=0; i < (fullPath.depth() - root.depth() -1); i++) {
        retPath = new Path(parent.getName(), retPath);
        parent = parent.getParent();
      }
      return new Path(justRoot, retPath);
    }
    return null;
  }

  public static void main(String[] args) throws URISyntaxException, IOException {
    
    Path cur = new Path(".");
    Path file1 = new Path("archive.har/part-0");
    FileSystem fs = FileSystem.get(new Configuration());
    FileStatus cur_status = fs.getFileStatus(cur);
    FileStatus file1_status = fs.getFileStatus(file1);
    System.out.println(cur_status);
    System.out.println(file1_status);
    
    URI cur_uri = cur_status.getPath().toUri();
    URI file1_uri = file1_status.getPath().toUri();
    System.out.println(cur_uri.relativize(file1_uri));
    
    // TODO Auto-generated method stub
    //URI uri = new URI("file://");
    //uri.re
    Path parent = new Path("file:/abc");
    System.out.println(parent);
    Path child = new Path("def");
    System.out.println(child);
    System.out.println(Path.mergePaths(parent, child));
    System.out.println(new Path(parent, child));
  }

}
