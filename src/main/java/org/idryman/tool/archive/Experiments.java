package org.idryman.tool.archive;

import java.io.IOException;
import java.net.URISyntaxException;
import java.util.Map.Entry;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.MapWritable;
import org.idryman.tool.fs.Har2FileStatus;

public class Experiments {

  public static void main(String[] args) throws URISyntaxException, IOException {
    
    FileSystem fs = FileSystem.get(new Configuration());
    Path indexPath = new Path("_index");
    Har2FileStatus har2Status = new Har2FileStatus();
    //MapWritable map = new MapWritable();
    FSDataInputStream fis = fs.open(indexPath);
    
    while(fis.available() > 0) {
      har2Status.readFields(fis);
      System.out.println(har2Status);
    }
  }

}
