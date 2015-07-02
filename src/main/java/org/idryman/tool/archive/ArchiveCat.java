package org.idryman.tool.archive;

import java.io.BufferedOutputStream;
import java.io.File;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.BasicConfigurator;
import org.tukaani.xz.SeekableFileInputStream;
import org.tukaani.xz.SeekableXZInputStream;

import com.google.common.base.Preconditions;

public class ArchiveCat extends Configured implements Tool{
  private static Log LOG = LogFactory.getLog(ArchiveCat.class);
  private static final int buf_len = 1024;
  private File in_file;
  private Configuration conf;
  public int run(String[] args) throws Exception {
    conf = getConf();
    String in = conf.get("archive.in");
    Preconditions.checkNotNull(in, "-Darchive.in=<file> is required");
    in_file = new File(in);
    Preconditions.checkState(in_file.exists(), "Input file doesn't exist");
    
    SeekableXZInputStream fis = new SeekableXZInputStream(new SeekableFileInputStream(in_file));
    LOG.info("Contains " + fis.getBlockCount() + " blocks");
    
    int block_num = Integer.parseInt(args[0]);
    fis.seekToBlock(block_num);
    BufferedOutputStream bo = new BufferedOutputStream(System.out);
    
    int block_len = (int)fis.getBlockSize(block_num);
    byte[] buffer = new byte[buf_len];
    while (block_len > buf_len) {
      fis.read(buffer, 0, buf_len);
      block_len -= buf_len;
      bo.write(buffer, 0, buf_len);
    }
    fis.read(buffer, 0, block_len);
    bo.write(buffer, 0, block_len);
    bo.flush();
    bo.close();
    fis.close();
    
    return 0;
  }
  
  public static void main(String[] args) throws Exception {
    BasicConfigurator.configure();
    System.exit(ToolRunner.run(new ArchiveCat(), args));
  }



}
