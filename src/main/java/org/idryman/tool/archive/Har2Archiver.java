package org.idryman.tool.archive;

import java.io.IOException;
import java.util.UUID;

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
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.BasicConfigurator;
import org.idryman.tool.fs.Har2FileStatus;
import org.tukaani.xz.FilterOptions;
import org.tukaani.xz.LZMA2Options;
import org.tukaani.xz.X86Options;
import org.tukaani.xz.XZOutputStream;

import com.google.common.base.Preconditions;

public final class Har2Archiver extends Configured implements Tool{
  public final static String HAR2_PARENT_KEY = "har2.archive.parent";
  public final static String HAR2_OUTPUT_KEY = "har2.archive.out";
  public final static String HAR2_JOB_UUID_KEY = "har2.job.uuid";
  public final static String HAR2_COMPRESSED_EXTENSION_KEY = "har2.archiver.compressed.extensions";
  public final static String COMB_MAX_SPLIT_KEY = "mapred.max.split.size";
  private static Log LOG = LogFactory.getLog(Har2Archiver.class);
  private Path outPath;
  private Path parentPath;
  
  
  public int run(String[] args) throws Exception {
    Configuration conf = getConf();
    FileSystem fs = FileSystem.get(conf);
    String outString = conf.get(HAR2_OUTPUT_KEY);
    String parentString = conf.get(HAR2_PARENT_KEY);
    Preconditions.checkNotNull(outString, "-D"+HAR2_OUTPUT_KEY+"=<archive output> is required");
    Preconditions.checkNotNull(parentString, "-D"+HAR2_PARENT_KEY+"=<parent path> is required");
    
    Path archivePath = fs.makeQualified(new Path(outString));
    if (!fs.exists(archivePath)) { 
      fs.mkdirs(archivePath);
      fs.create(new Path(archivePath, "_HAR2_")).close();
    }
    
    String jobUUID = "har2-" + UUID.randomUUID();
    conf.set("har2.job.uuid", jobUUID);
    
    outPath    = new Path(archivePath, jobUUID);
    parentPath = fs.makeQualified(new Path(parentString));
    // set default combine split size 4GB
    long maxSize = conf.getLong(COMB_MAX_SPLIT_KEY, 4*1024*1024*1024L);
    LOG.info("Archive input max split size is: "+maxSize);
    conf.setLong(COMB_MAX_SPLIT_KEY, maxSize);
    
    //MultipleInputs.

    Job job = Job.getInstance(conf);
    job.setJobName("Har2 Archiever. Parent path: "+parentString);
    job.setJarByClass(Har2Archiver.class);
    job.setInputFormatClass(ArchiveInputFormat.class);
    job.setMapperClass(NonCompressedFileMapper.class);
    job.setNumReduceTasks(0);
    
    //ArchiveInputFormat inputFormat = new ArchiveInputFormat();
    fs.setWorkingDirectory(parentPath);
    for (String arg : args) {
      Path path = fs.makeQualified(new Path(arg));
      ArchiveInputFormat.addInputPath(job, path);
    }
    FileOutputFormat.setOutputPath(job, outPath);
    //job.se
    job.submit();
   
    
    if (job.waitForCompletion(true)) {
      
      LOG.info("Archive almost complete, now creating the directory map");
      
//      MapWritable dirMap = new MapWritable();
//      FSDataOutputStream dirIndexOut = fs.create(new Path(outPath, "index-m-alldirs"));
//      FSDataOutputStream dirMapOut = fs.create(new Path(outPath, "directoryMap"));
//      
//      fs.setWorkingDirectory(parentPath);
//      Har2FileStatus har2Status;
//      ArrayList<Har2FileStatus> har2Arr = Lists.newArrayList(); 
//      for (String arg : args) {
//        Path path = fs.makeQualified(new Path(arg));
//        for (FileStatus status : fs.globStatus(path)) {
//          har2Status = new Har2FileStatus(status, parentPath);
//          har2Arr.add(har2Status);
//        }
//      }
//      har2Status = new Har2FileStatus(fs.getFileStatus(parentPath),parentPath);
//       // Path would write empty string if it were "." Need to use this work around to fake the current directory
//      har2Status.setPath(new Path("..")); 
//      har2Status.write(dirIndexOut);
//      
//      dirMap.put(new Text(""), new Har2ArrayWritable(har2Arr.toArray(new Har2FileStatus[0])));
//      
//      for (FileStatus status : archiveInput.listDirectoryStatus(job)) {
//        //LOG.debug("adding path: " + status.getPath());
//        har2Status = new Har2FileStatus(status, parentPath);
//        har2Status.write(dirIndexOut);
//        Text key = new Text(har2Status.getPath().toString());
//        ArrayList<Har2FileStatus> tmpValues = Lists.newArrayList(); 
//        for (FileStatus s : fs.listStatus(status.getPath())) {
//          tmpValues.add(new Har2FileStatus(s, parentPath));
//        }
//        ArrayWritable value = new Har2ArrayWritable(tmpValues.toArray(new Har2FileStatus[0]));
//        dirMap.put(key, value);
//      }
//      dirIndexOut.close();
//      
//      try {
//        dirMap.write(dirMapOut);
//      } catch(Exception e) {
//        LOG.error("Couldn't create directoryMap");
//        e.printStackTrace();
//      } finally {
//        dirMapOut.close();
//      }
//      // touch _HAR2_
//      fs.create(new Path(outPath, "_HAR2_")).close();
      
      return 0;
    }
    return 1;
  }
  
  public final static class NonCompressedFileMapper extends Mapper<FileStatus, Har2FileStatus, NullWritable, NullWritable> {
    private final static Log    LOG                     = LogFactory.getLog(NonCompressedFileMapper.class);
    private final static String XZ_PARTITION_BASE_NAME  = "xz-partition";
    private final static String CAT_PARTITION_BASE_NAME = "cat-partition";
    private final static String INDEX_BASE_NAME         = "index";
    private final static int    XZ_COMPRESS_LEVEL       = 6;
    private String []           extentions;
    private FileSystem          fs;
    private Configuration       conf;
    private Path                catPartitionPath;
    private Path                xzPartitionPath;
    private long                xzBlockId;
    private long                catOffset;
    private FSDataOutputStream  catOutStream;
    private XZOutputStream      xzOutStream;
    private FSDataOutputStream  indexOutStream;
    
    private static enum StatusType {
      COMPRESSED_FILE, NON_COMPRESSED_FILE, EMPTY_FILE, DIRECTORY;
    };
    
    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
      conf = context.getConfiguration();
      fs = FileSystem.get(conf);
      extentions = conf.get(HAR2_COMPRESSED_EXTENSION_KEY, ".gz,.bz2,.lzo,.snappy").split(",");
      
      catPartitionPath = FileOutputFormat.getPathForWorkFile(context, CAT_PARTITION_BASE_NAME, "");
      xzPartitionPath  = FileOutputFormat.getPathForWorkFile(context, XZ_PARTITION_BASE_NAME, ".xz");
      Path indexPath        = FileOutputFormat.getPathForWorkFile(context, INDEX_BASE_NAME, "");
     
      X86Options x86 = new X86Options();
      LZMA2Options lzma2 = new LZMA2Options(XZ_COMPRESS_LEVEL);
      FilterOptions[] options = { x86, lzma2 };
      LOG.info("XZ Encoder memory usage: "
              + FilterOptions.getEncoderMemoryUsage(options)
              + " KiB");

      xzOutStream      = new XZOutputStream(fs.create(xzPartitionPath), options);
      catOutStream     = fs.create(catPartitionPath);
      indexOutStream   = fs.create(indexPath);
      xzBlockId        = 0;
      catOffset        = 0;
    }
    
    @Override
    protected void map(FileStatus srcStat, Har2FileStatus dstStat, Context context) 
        throws IOException, InterruptedException {
      LOG.debug("Processing file: " + srcStat.getPath());
      long bytesCopied;
      FSDataInputStream in;
      
      switch(getStatusType(srcStat)) {
      case COMPRESSED_FILE:
        in = fs.open(srcStat.getPath());
        bytesCopied = IOUtils.copyLarge(in, catOutStream);
        Preconditions.checkState(bytesCopied == srcStat.getLen(), "File status byte: " + srcStat.getLen() + " should match byteCopied: " + bytesCopied);
        LOG.info(String.format("Copied %d bytes from %s to %s", 
            bytesCopied, srcStat.getPath().toString(), catPartitionPath.getName())); 
        dstStat.setPartitionAndOffset(catPartitionPath.getName(), catOffset);
        catOffset+=bytesCopied;
        in.close();
        break;
      case NON_COMPRESSED_FILE:
        in = fs.open(srcStat.getPath());
        bytesCopied = IOUtils.copyLarge(in, xzOutStream);
        xzOutStream.flush();
        xzOutStream.endBlock();
        LOG.info(String.format("Copied %d bytes from %s to %s", 
            bytesCopied, srcStat.getPath().toString(), xzPartitionPath.getName()));
        dstStat.setPartitionAndOffset(xzPartitionPath.getName(), xzBlockId++);
        in.close();
        break;
      case EMPTY_FILE:
        dstStat.setPartitionAndOffset("", -1);
        break;
      case DIRECTORY:
      }
      
      dstStat.write(indexOutStream);
      indexOutStream.flush();
    }
    
    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
      fs.delete(FileOutputFormat.getPathForWorkFile(context, "part", ""), false);
      catOutStream.close();
      xzOutStream.close();
      indexOutStream.close();
      
      if (xzBlockId == 0) {
        fs.delete(xzPartitionPath, false);
      }
      if (catOffset == 0) {
        fs.delete(catPartitionPath, false);
      }
    }
    
    @SuppressWarnings("resource")
    private StatusType getStatusType(FileStatus status) throws IOException {
      if (status.isDirectory()) return StatusType.DIRECTORY;
      if (status.getLen() == 0) return StatusType.EMPTY_FILE;
      
      String fileName = status.getPath().getName();
      for (String extention : extentions) {
        if (fileName.endsWith(extention)) {
          return StatusType.COMPRESSED_FILE;
        }
      }
      if (fileName.endsWith(".seq") &&
          new SequenceFile.Reader(conf, 
              SequenceFile.Reader.file(status.getPath())).isCompressed()) {
        return StatusType.COMPRESSED_FILE;
      }
      
      return StatusType.NON_COMPRESSED_FILE;
    }
  }

  
  public static void main(String[] args) throws Exception {
    BasicConfigurator.configure();
    System.exit(ToolRunner.run(new Har2Archiver(), args));
  }

}
