package org.idryman.tool.archive;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.List;

import org.apache.commons.io.IOUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.BasicConfigurator;
import org.idryman.tool.fs.Har2FileStatus;
import org.tukaani.xz.FilterOptions;
import org.tukaani.xz.LZMA2Options;
import org.tukaani.xz.X86Options;
import org.tukaani.xz.XZOutputStream;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;

public final class Archiver extends Configured implements Tool{
  private static Log LOG = LogFactory.getLog(Archiver.class);
  private Path out_file;
  private Configuration conf;
  private List<Path> in_files;
  
  public int run(String[] args) throws Exception {
    init(args);
    archive();
    return 0;
  }
  
  private void init(String args[]) throws IOException {
    conf = getConf();
    FileSystem fs = FileSystem.get(conf);
    String out = conf.get("archive.out");
    Preconditions.checkNotNull(out, "-Darchive.out=<file> is required");
    out_file = new Path(out);

    in_files = Lists.newArrayList();
    
    for (String arg : args) {
      Path f = new Path(arg);
      if (!fs.exists(f)) {
        LOG.warn("File doesn't exist: " + f + " skipping.");
      } else if (fs.isDirectory(f)) {
        LOG.warn(f + " is a directory. skipping");
      } else {
        in_files.add(f);
      }
    }
  }
  
  private void archive() throws FileNotFoundException, IOException {
    int level = conf.getInt("archive.xz.level", 6);
    FileSystem fs = FileSystem.get(conf);
    
    X86Options x86 = new X86Options();
    LZMA2Options lzma2 = new LZMA2Options(level);
    FilterOptions[] options = { x86, lzma2 };
    LOG.info("Encoder memory usage: "
            + FilterOptions.getEncoderMemoryUsage(options)
            + " KiB");
    LOG.info("Decoder memory usage: "
            + FilterOptions.getDecoderMemoryUsage(options)
            + " KiB");
    
    XZOutputStream outxz = new XZOutputStream(fs.create(out_file, true), options);
    
    //Har2FileStatus f_status = Har2FileStatus.newFileStatusWithBlockId(f, blockId)
    FSDataOutputStream indexOut = fs.create(new Path("_index"), true);
    /*
     * TODO although MapWritable is convenient, but maybe not that flexible to build a
     * har:/index -> actual FileStatus map on reading time
     */
    
    Path parent = fs.getFileStatus(new Path(".")).getPath();
    
    //MapWritable indexMap = new MapWritable();
    
    int blockId = 0;
    for (Path f : in_files) {
      InputStream fis = fs.open(f);
      FileStatus status = fs.getFileStatus(f);

      // TODO check what would happen if input file is empty
      // Maybe need to set the block number to -1 when it's empty
      /*
       *  When the file is zero byte, need to do special case:
       *  1. still need to store file status
       *  2. Do not advance outxz
       *  3. on har2filesystem, return 0 byte
       *  4. blockId is -1
       *  5. part name is ""
       */
      Har2FileStatus har2Status = new Har2FileStatus(status);

      
      if (har2Status.getLen() == 0) {
        har2Status.setPartitionAndBlock("", -1);
        LOG.info("File " + f + " has 0 length. Created a pseudo fileStatus");
      } else {
        
        // TODO need to handle the case when the bytes to copy is too large
        // need to either use copyLarge, or incremental copy?
        LOG.info("Copied " + IOUtils.copy(fis, outxz) + " bytes from " + f);
        outxz.flush();
        outxz.endBlock();
        har2Status.setPartitionAndBlock("part-0", blockId++);
      }
      har2Status.makeRelativeHar2Status(parent);
      har2Status.write(indexOut);

      

      //indexMap.put(new Text(har2Status.getPath().toString()), har2Status);
      
      fis.close();
    }
    //indexMap.write(indexOut);
    outxz.close();
    indexOut.close();
  }
  
  public static void main(String[] args) throws Exception {
    BasicConfigurator.configure();
    System.exit(ToolRunner.run(new Archiver(), args));
  }

}
