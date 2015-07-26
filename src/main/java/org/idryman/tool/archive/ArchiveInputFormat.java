package org.idryman.tool.archive;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.CombineFileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.CombineFileSplit;
import org.idryman.tool.fs.Har2FileStatus;

import com.google.common.collect.Lists;

public final class ArchiveInputFormat extends CombineFileInputFormat<FileStatus, Har2FileStatus> {
  private final static Log LOG = LogFactory.getLog(ArchiveInputFormat.class);
  private List<FileStatus> allStatuses;
  
  /*
   * Currently no file get split by codec, but we can imporve that later on.
   */
  @Override
  protected boolean isSplitable(JobContext context, Path file) {
    return false;
  }
  
  @Override
  public List<FileStatus> listStatus(JobContext job) throws IOException {
    return listFileStatus(job);
  }
  
  public List<FileStatus> listFileStatus(JobContext job) throws IOException {
    if (allStatuses==null) { buildAllStatuses(job); }
    List<FileStatus> ret = Lists.newArrayList();
    for (FileStatus status : allStatuses) {
      if (status.isFile()) {
        ret.add(status);
      }
    }
    return ret;
  }
  
  public List<FileStatus> listDirectoryStatus(JobContext job) throws IOException {
    if (allStatuses==null) { buildAllStatuses(job); }
    List<FileStatus> ret = Lists.newArrayList();
    for (FileStatus status : allStatuses) {
      if (status.isDirectory()) {
        ret.add(status);
      }
    }
    return ret;
  }
  
  private void buildAllStatuses(JobContext job) throws IOException {
    FileSystem fs = FileSystem.get(job.getConfiguration());
    allStatuses = Lists.newArrayList();
    for (Path inPath : getInputPaths(job)) {
      for (FileStatus status : fs.globStatus(inPath)) {
        addStatusesRecursively(status, fs, allStatuses);
      }
    }
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
      InputSplit split, TaskAttemptContext context) throws IOException {
    return new ArchiverRecordReader(split, context);
  }
  
  
  public static class ArchiverRecordReader extends RecordReader<FileStatus, Har2FileStatus>{
    private ArrayList<FileStatus>     fileStatuses;
    private ArrayList<Har2FileStatus> har2Statuses;
    int index, length;
    float bytesRead, totalBytes;
    
    public ArchiverRecordReader(InputSplit split, TaskAttemptContext context) 
        throws IOException {
      initialize(split, context);
    }
    
    @Override
    public void initialize(InputSplit split, TaskAttemptContext context)
        throws IOException {
      Configuration conf = context.getConfiguration();
      FileSystem fs = FileSystem.get(conf);
      CombineFileSplit comSplit = (CombineFileSplit) split;
      
      Path parent = fs.makeQualified(new Path(conf.get(Archiver.HAR2_PARENT_KEY)));
      
      bytesRead = totalBytes = 0;
      fileStatuses = Lists.newArrayList();
      har2Statuses = Lists.newArrayList();
      for (Path path : comSplit.getPaths()) {
        FileStatus     status     = fs.getFileStatus(path);
        Har2FileStatus har2Status = new Har2FileStatus(status, parent);
        fileStatuses.add(status);
        har2Statuses.add(har2Status);
        totalBytes += status.getLen();
      }
      length = fileStatuses.size();
      index = -1;
    }

    @Override
    public boolean nextKeyValue() throws IOException, InterruptedException {
      index++;
      if (index > 0)
        bytesRead += fileStatuses.get(index-1).getLen();
      return index < length;
    }

    @Override
    public FileStatus getCurrentKey() throws IOException,
        InterruptedException {
      return fileStatuses.get(index);
    }

    @Override
    public Har2FileStatus getCurrentValue() throws IOException,
        InterruptedException {
      return har2Statuses.get(index);
    }

    @Override
    public float getProgress() throws IOException, InterruptedException {
      return bytesRead / totalBytes; 
    }

    @Override
    public void close() throws IOException {}
    
  }
}
