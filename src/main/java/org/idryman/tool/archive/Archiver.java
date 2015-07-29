package org.idryman.tool.archive;

import java.io.IOException;
import java.util.ArrayList;

import org.apache.commons.io.IOUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.BasicConfigurator;
import org.idryman.tool.fs.Har2ArrayWritable;
import org.idryman.tool.fs.Har2FileStatus;
import org.tukaani.xz.FilterOptions;
import org.tukaani.xz.LZMA2Options;
import org.tukaani.xz.X86Options;
import org.tukaani.xz.XZOutputStream;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;

public final class Archiver extends Configured implements Tool{
  public final static String HAR2_PARENT_KEY = "archive.parent";
  public final static String HAR2_OUTPUT_KEY = "archive.out";
  public final static String COMB_MAX_SPLIT_KEY = "mapred.max.split.size";
  private static Log LOG = LogFactory.getLog(Archiver.class);
  private Path outPath;
  private Path parentPath;
  
  
  public int run(String[] args) throws Exception {
    Configuration conf = getConf();
    FileSystem fs = FileSystem.get(conf);
    String outString = conf.get(HAR2_OUTPUT_KEY);
    String parentString = conf.get(HAR2_PARENT_KEY);
    Preconditions.checkNotNull(outString, "-D"+HAR2_OUTPUT_KEY+"=<archive output> is required");
    Preconditions.checkNotNull(parentString, "-D"+HAR2_PARENT_KEY+"=<parent path> is required");
    outPath    = fs.makeQualified(new Path(outString));
    parentPath = fs.makeQualified(new Path(parentString));
    // set default combine split size 4GB
    long maxSize = conf.getLong(COMB_MAX_SPLIT_KEY, 4*1024*1024*1024L);
    LOG.info("Archive input max split size is: "+maxSize);
    conf.setLong(COMB_MAX_SPLIT_KEY, maxSize);

    Job job = Job.getInstance(conf);
    job.setJobName("Har2 Archiever. Parent path: "+parentString);
    job.setJarByClass(Archiver.class);
    job.setInputFormatClass(ArchiveInputFormat.class);
    job.setMapperClass(ArchiveMapper.class);
    job.setNumReduceTasks(0);
    
    fs.setWorkingDirectory(parentPath);
    ArchiveInputFormat archiveInput = new ArchiveInputFormat();
    for (String arg : args) {
      Path path = fs.makeQualified(new Path(arg));
      ArchiveInputFormat.addInputPath(job, path);
    }
    FileOutputFormat.setOutputPath(job, outPath);
    
    job.submit();
    
    if (job.waitForCompletion(true)) {
      
      LOG.info("Archive almost complete, now creating the directory map");
      
      MapWritable dirMap = new MapWritable();
      FSDataOutputStream dirIndexOut = fs.create(new Path(outPath, "index-m-alldirs"));
      FSDataOutputStream dirMapOut = fs.create(new Path(outPath, "directoryMap"));
      
      fs.setWorkingDirectory(parentPath);
      Har2FileStatus har2Status;
      ArrayList<Har2FileStatus> har2Arr = Lists.newArrayList(); 
      for (String arg : args) {
        Path path = fs.makeQualified(new Path(arg));
        for (FileStatus status : fs.globStatus(path)) {
          har2Status = new Har2FileStatus(status, parentPath);
          har2Arr.add(har2Status);
        }
      }
      har2Status = new Har2FileStatus(fs.getFileStatus(parentPath),parentPath);
       // Path would write empty string if it were "." Need to use this work around to fake the current directory
      har2Status.setPath(new Path("..")); 
      har2Status.write(dirIndexOut);
      
      dirMap.put(new Text(""), new Har2ArrayWritable(har2Arr.toArray(new Har2FileStatus[0])));
      
      for (FileStatus status : archiveInput.listDirectoryStatus(job)) {
        //LOG.debug("adding path: " + status.getPath());
        har2Status = new Har2FileStatus(status, parentPath);
        har2Status.write(dirIndexOut);
        Text key = new Text(har2Status.getPath().toString());
        ArrayList<Har2FileStatus> tmpValues = Lists.newArrayList(); 
        for (FileStatus s : fs.listStatus(status.getPath())) {
          tmpValues.add(new Har2FileStatus(s, parentPath));
        }
        ArrayWritable value = new Har2ArrayWritable(tmpValues.toArray(new Har2FileStatus[0]));
        dirMap.put(key, value);
      }
      dirIndexOut.close();
      
      try {
        dirMap.write(dirMapOut);
      } catch(Exception e) {
        LOG.error("Couldn't create directoryMap");
        e.printStackTrace();
      } finally {
        dirMapOut.close();
      }
      // touch _HAR2_
      fs.create(new Path(outPath, "_HAR2_")).close();
      
      return 0;
    }
    return 1;
  }
  
  public final static class ArchiveMapper extends Mapper<FileStatus, Har2FileStatus, NullWritable, NullWritable> {
    private final static Log    LOG                 = LogFactory.getLog(ArchiveMapper.class);
    private final static String PARTITION_BASE_NAME = "partition";
    private final static String INDEX_BASE_NAME     = "index";
    private final static int XZ_COMPRESS_LEVEL      = 6;
    private FileSystem         fs;
    private String             partitionName;
    private int                blockId;
    private XZOutputStream     xzOutStream;
    private FSDataOutputStream indexOutStream;
    
    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
      fs = FileSystem.get(context.getConfiguration());
      Path partitionPath = FileOutputFormat.getPathForWorkFile(context, PARTITION_BASE_NAME, ".xz");
      Path indexPath     = FileOutputFormat.getPathForWorkFile(context, INDEX_BASE_NAME, "");
     
      X86Options x86 = new X86Options();
      LZMA2Options lzma2 = new LZMA2Options(XZ_COMPRESS_LEVEL);
      FilterOptions[] options = { x86, lzma2 };
      LOG.info("XZ Encoder memory usage: "
              + FilterOptions.getEncoderMemoryUsage(options)
              + " KiB");
      LOG.info("XZ Decoder memory usage: "
              + FilterOptions.getDecoderMemoryUsage(options)
              + " KiB");
      
      partitionName  = partitionPath.getName();
      xzOutStream    = new XZOutputStream(fs.create(partitionPath), options);
      indexOutStream = fs.create(indexPath);
      blockId       = 0;
    }
    
    @Override
    protected void map(FileStatus srcStat, Har2FileStatus dstStat, Context context) 
        throws IOException, InterruptedException {
      LOG.debug("Processing file: " + srcStat.getPath());
      if (srcStat.getLen() > 0) {
        FSDataInputStream in = fs.open(srcStat.getPath());
        int bytesCopied = IOUtils.copy(in, xzOutStream);
        xzOutStream.flush();
        xzOutStream.endBlock();
        LOG.info(String.format("Copied %d bytes from %s to %s", 
            bytesCopied, srcStat.getPath().toString(), partitionName));        
        dstStat.setPartitionAndBlock(partitionName, blockId++);
      } else {
        // for 0 lenth file, it has no partition nor blockId;
        dstStat.setPartitionAndBlock("", -1);
      }
      dstStat.write(indexOutStream);
      indexOutStream.flush();
    }
    
    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
      fs.delete(FileOutputFormat.getPathForWorkFile(context, "part", ""), false);
      xzOutStream.close();
      indexOutStream.close();
    }
  }
  

  
  public static void main(String[] args) throws Exception {
    BasicConfigurator.configure();
    System.exit(ToolRunner.run(new Archiver(), args));
  }

}
