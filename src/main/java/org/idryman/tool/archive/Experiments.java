package org.idryman.tool.archive;

import java.io.IOException;
import java.net.URISyntaxException;
import java.util.Map.Entry;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.MapWritable;

public class Experiments {

  public static void main(String[] args) throws URISyntaxException, IOException {
    
    FileSystem fs = FileSystem.get(new Configuration());
    Path indexPath = new Path("_index");
    MapWritable map = new MapWritable();
    map.readFields(fs.open(indexPath));
    
    for (Entry entry : map.entrySet()) {
      System.out.println(entry.getKey());
      System.out.println(entry.getValue());
    }
  }

}
