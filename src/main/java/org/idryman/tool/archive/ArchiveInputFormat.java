package org.idryman.tool.archive;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.CombineFileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.CombineFileSplit;
import org.apache.hadoop.util.ReflectionUtils;
import org.idryman.tool.fs.Har2FileStatus;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;

public class ArchiveInputFormat extends CombineFileInputFormat<FileStatus, Har2FileStatus> {
  private final static Log LOG = LogFactory.getLog(ArchiveInputFormat.class);
  
  @Override
  protected final boolean isSplitable(JobContext context, Path file) {
    return false;
  }
  
  @Override
  public final RecordReader<FileStatus, Har2FileStatus> createRecordReader(
      InputSplit split, TaskAttemptContext context) throws IOException {
    return new ArchiverRecordReader(split, context);
  }
  
  @Override
  public final List<FileStatus> listStatus(JobContext job) throws IOException {
    List<FileStatus> result = new ArrayList<FileStatus>();
    FileSystem fs = FileSystem.get(job.getConfiguration());
    NonSymlinkFilter filter = ReflectionUtils.newInstance(NonSymlinkFilter.class, job.getConfiguration());
    
    for (Path p : getInputPaths(job)) {
      FileStatus[] matches = fs.globStatus(p);
      Preconditions.checkNotNull(matches, "File not found: " + p);
      for (FileStatus status : matches){
        if (filter.accept(status.getPath())) {
          addInputPathRecursively(result, fs, status.getPath(), filter);
        }
      }
    }
    
    return result;
  }
  
  public static class NonSymlinkFilter implements PathFilter, Configurable {
    protected Configuration conf;
    public NonSymlinkFilter(){};
    public boolean accept(Path path) {
      try {
        FileSystem fs = FileSystem.get(conf);
        return !fs.getFileStatus(path).isSymlink();
      }  catch (IOException e) {
        e.printStackTrace();
      }
      return false;
    } 

    public void setConf(Configuration conf) {
      this.conf = conf;
    }

    public Configuration getConf() {
      return conf;
    }
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
      
      Path parent = fs.makeQualified(new Path(conf.get(Har2Archiver.HAR2_PARENT_KEY)));
      String jobUUID = conf.get(Har2Archiver.HAR2_JOB_UUID_KEY);
      
      bytesRead = totalBytes = 0;
      fileStatuses = Lists.newArrayList();
      har2Statuses = Lists.newArrayList();
      for (Path path : comSplit.getPaths()) {
        FileStatus     status     = fs.getFileStatus(path);
        Har2FileStatus har2Status = new Har2FileStatus(status, jobUUID, parent);
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
