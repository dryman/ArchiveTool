package org.idryman.tool.archive;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

import org.apache.hadoop.fs.Path;

public class Experiments {
  

  public static void main(String[] args) throws URISyntaxException, IOException {

//    FileSystem fs = FileSystem.get(new Configuration());
//    Path indexPath = new Path("_index");
//    Har2FileStatus har2Status = new Har2FileStatus();
//    //MapWritable map = new MapWritable();
//    FSDataInputStream fis = fs.open(indexPath);
//    
//    while(fis.available() > 0) {
//      har2Status.readFields(fis);
//      System.out.println(har2Status);
//    }
    URI uri = new URI("har2://my-scheme!host:123/path1/path2");
    System.out.println(uri.getScheme());
    System.out.println(uri.getAuthority());
    System.out.println(uri.getUserInfo());
    System.out.println(uri.getHost());
    System.out.println(uri.getPort());
    Path p = new Path(uri);
    System.out.println(p);
    
    uri = new URI("file:/abc");
    URI relativeURI = uri.relativize(new URI("file:/abc"));
    System.out.println(relativeURI.getScheme());
    System.out.println(relativeURI.getAuthority());
    System.out.println(relativeURI.getUserInfo());
    System.out.println(relativeURI.getHost());
    System.out.println(relativeURI.getPort());
    System.out.println(relativeURI.getPath());
    System.out.println("---");
    
    uri = new URI("http://def.com/abc");
    URI relativeURI2 = uri.relativize(new URI("http://def.com/abc"));
    System.out.println(relativeURI.equals(relativeURI2));
  }

}
