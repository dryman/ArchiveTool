package org.idryman.tool.archive;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Collections;
import java.util.List;

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
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
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
import com.google.common.collect.Lists;

public final class Archiver extends Configured implements Tool{
  private static Log LOG = LogFactory.getLog(Archiver.class);
  private Path outPath;
  private Path parentPath;
  private Configuration conf;
  private List<Path> in_files;
  
  public int run(String[] args) throws Exception {
    conf = getConf();
    FileSystem fs = FileSystem.get(conf);
    String outString = conf.get("archive.out");
    String parentString = conf.get("archive.parent");
    Preconditions.checkNotNull(outString, "-Darchive.out=<archive output> is required");
    Preconditions.checkNotNull(parentString, "-Darchive.parent=<parent path> is required");
    outPath    = fs.makeQualified(new Path(outString));
    parentPath = fs.makeQualified(new Path(parentString));

    Job job = new Job(conf);
    
    fs.setWorkingDirectory(parentPath);
    //
    for (String arg : args) {
      Path path = fs.makeQualified(new Path(arg));
      ArchiveInputFormat.addInputPath(job, path);
    }
    
    FileOutputFormat.setOutputPath(job, outPath);
    // job.run
    // job.waitForCompletion()
    // expose listStatus recursively from ArchiveInputFormat
    // for each status that is dir, add a new entry to
    // MapWritable<Text,ArrayWritable<Har2FileStatus>>
    // and write to outPath
    archive();
    return 0;
  }
  

  
  public final static class ArchiveInputFormat extends FileInputFormat<FileStatus, Har2FileStatus> {
    private final static Log LOG = LogFactory.getLog(ArchiveInputFormat.class);
    private List<FileStatus> inputStatuses;
    @Override
    public List<InputSplit> getSplits(JobContext job) throws IOException {
      // probably just use combinefilesplit
      // for each file status, get blockinfo and get locations and pass on to combinefilesplit
      return super.getSplits(job);
      
    }
    
    public List<FileStatus> getStatusesRecursively (JobContext job) throws IOException {
      if (inputStatuses==null) {
        FileSystem fs = FileSystem.get(job.getConfiguration());
        inputStatuses = Lists.newArrayList();
        for (Path inPath : getInputPaths(job)) {
          for (FileStatus status : fs.globStatus(inPath)) {
            addStatusesRecursively(status, fs, inputStatuses);
          }
        }
      }
      
      return inputStatuses;
    }
    
    private void addStatusesRecursively (FileStatus f, FileSystem fs, List<FileStatus> accumlator) throws FileNotFoundException, IOException{
      for (FileStatus stat : fs.listStatus(f.getPath())) {
        if (stat.isSymlink()) {
          LOG.warn("skiping symlink: " + stat.getPath());
          continue;
        }
        accumlator.add(stat);
        if (stat.isDirectory()) {
          addStatusesRecursively(stat, fs, accumlator);
        }
      }
    }
    
    @Override
    public RecordReader<FileStatus, Har2FileStatus> createRecordReader(
        InputSplit arg0, TaskAttemptContext arg1) throws IOException,
        InterruptedException {
      // TODO Auto-generated method stub
      return null;
    }
    
    public final static class ArchiveInputSplit extends InputSplit {
      List<FileStatus> statuses;
      long length;
      @Override
      public long getLength() throws IOException, InterruptedException {
        // TODO Auto-generated method stub
        return 0;
      }

      @Override
      public String[] getLocations() throws IOException, InterruptedException {
        // from fs.getBlockLocation, each location give you the hosts
        return null;
      }
      
    }
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
    }
    
    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
      xzOutStream.close();
      indexOutStream.close();
    }
  }
  
  
  private void archive() throws FileNotFoundException, IOException {
    int level = conf.getInt("archive.xz.level", 6);
    FileSystem fs = FileSystem.get(conf);
    
//    X86Options x86 = new X86Options();
//    LZMA2Options lzma2 = new LZMA2Options(level);
//    FilterOptions[] options = { x86, lzma2 };
//    LOG.info("Encoder memory usage: "
//            + FilterOptions.getEncoderMemoryUsage(options)
//            + " KiB");
//    LOG.info("Decoder memory usage: "
//            + FilterOptions.getDecoderMemoryUsage(options)
//            + " KiB");
    
//    XZOutputStream outxz = new XZOutputStream(fs.create(out_file, true), options);
//    
//    //Har2FileStatus f_status = Har2FileStatus.newFileStatusWithBlockId(f, blockId)
//    FSDataOutputStream indexOut = fs.create(new Path("_index"), true);
    /*
     * TODO although MapWritable is convenient, but maybe not that flexible to build a
     * har:/index -> actual FileStatus map on reading time
     */
    
    Path parent = fs.getFileStatus(new Path(".")).getPath();
    
    //MapWritable indexMap = new MapWritable();
    
    int blockId = 0;
    List<FileStatus> list = Lists.newArrayList();
    for (Path f : in_files) {
      //InputStream fis = fs.open(f);
      FileStatus [] status = fs.globStatus(f);

      
//      for (FileStatus s : fs.globStatus(f)) {
//        addStatusesRecursively(s, fs, list);
//      }
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
//      Har2FileStatus har2Status = new Har2FileStatus(status);
//
//      
//      if (har2Status.getLen() == 0) {
//        har2Status.setPartitionAndBlock("", -1);
//        LOG.info("File " + f + " has 0 length. Created a pseudo fileStatus");
//      } else {
//        
//        // TODO need to handle the case when the bytes to copy is too large
//        // need to either use copyLarge, or incremental copy?
//        LOG.info("Copied " + IOUtils.copy(fis, outxz) + " bytes from " + f);
//        outxz.flush();
//        outxz.endBlock();
//        har2Status.setPartitionAndBlock("part-0", blockId++);
//      }
//      har2Status.makeRelativeHar2Status(parent);
//      har2Status.write(indexOut);
//
//      
//
//      //indexMap.put(new Text(har2Status.getPath().toString()), har2Status);
//      
//      fis.close();
    }
    Collections.sort(list);
    for (FileStatus s : list) {
      System.out.println(s.getPath());
    }
    //indexMap.write(indexOut);
//    outxz.close();
//    indexOut.close();
    
  }
  

  
  public static void main(String[] args) throws Exception {
    BasicConfigurator.configure();
    System.exit(ToolRunner.run(new Archiver(), args));
  }

}
