package org.idryman.tool.archive;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.List;

import org.apache.commons.io.IOUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.BasicConfigurator;
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
    OutputStream index = fs.create(new Path("_index"), true);
    
    for (Path f : in_files) {
      InputStream fis = fs.open(f);
      LOG.debug("Path: " + f);
      LOG.debug("File status: " + fs.getFileStatus(f));
      LOG.info("Copied " + IOUtils.copy(fis, outxz) + " bytes from " + f);
      outxz.flush();
      outxz.endBlock();
      fis.close();
    }
    outxz.close();
    index.close();
  }
  
  public static void main(String[] args) throws Exception {
    BasicConfigurator.configure();
    System.exit(ToolRunner.run(new Archiver(), args));
  }

}
