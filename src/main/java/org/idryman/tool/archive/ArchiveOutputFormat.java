package org.idryman.tool.archive;

import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.OutputCommitter;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputCommitter;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class ArchiveOutputFormat extends FileOutputFormat<NullWritable, NullWritable>{
  private OutputCommitter committer;
  
  @Override
  public RecordWriter<NullWritable, NullWritable> getRecordWriter(
      TaskAttemptContext job) throws IOException, InterruptedException {
    return null;
  }

  @Override
  public synchronized 
  OutputCommitter getOutputCommitter(TaskAttemptContext context) throws IOException {
   if (committer == null) {
     Path output = getOutputPath(context);
     committer = new Har2OutputCommitter(output, context);
   }
   return committer;
  }
  
  public static class Har2OutputCommitter extends FileOutputCommitter {

    public Har2OutputCommitter(Path outputPath, TaskAttemptContext context) throws IOException {
      super(outputPath, (JobContext)context);
    }

    @Override
    public void commitJob(JobContext context) throws IOException {
      Path[] inputPaths = FileInputFormat.getInputPaths(context);
      Path outputPath = getOutputPath(context);
      
      /*
       * Added path: path_l2, should be done in mapper already
       * Should forbidden nested path added path_l1, path_l1/path_l2
       * Should forbidden same path added in
       * path_l1/path_l2
       * parent -> ..
       * find out all the possible l1..
       * If l2 has other siblings, should ignore
       * 
       * path_l1/*
       * -> should apply glob before do anything
       * 
       * need to exclude paths that are already included in previous output.. 
       */
      
//    MapWritable dirMap = new MapWritable();
//    FSDataOutputStream dirIndexOut = fs.create(new Path(outPath, "index-m-alldirs"));
//    FSDataOutputStream dirMapOut = fs.create(new Path(outPath, "directoryMap"));
//    
//    fs.setWorkingDirectory(parentPath);
//    Har2FileStatus har2Status;
//    ArrayList<Har2FileStatus> har2Arr = Lists.newArrayList(); 
//    for (String arg : args) {
//      Path path = fs.makeQualified(new Path(arg));
//      for (FileStatus status : fs.globStatus(path)) {
//        har2Status = new Har2FileStatus(status, parentPath);
//        har2Arr.add(har2Status);
//      }
//    }
//    har2Status = new Har2FileStatus(fs.getFileStatus(parentPath),parentPath);
//     // Path would write empty string if it were "." Need to use this work around to fake the current directory
//    har2Status.setPath(new Path("..")); 
//    har2Status.write(dirIndexOut);
//    
//    dirMap.put(new Text(""), new Har2ArrayWritable(har2Arr.toArray(new Har2FileStatus[0])));
//    
//    for (FileStatus status : archiveInput.listDirectoryStatus(job)) {
//      //LOG.debug("adding path: " + status.getPath());
//      har2Status = new Har2FileStatus(status, parentPath);
//      har2Status.write(dirIndexOut);
//      Text key = new Text(har2Status.getPath().toString());
//      ArrayList<Har2FileStatus> tmpValues = Lists.newArrayList(); 
//      for (FileStatus s : fs.listStatus(status.getPath())) {
//        tmpValues.add(new Har2FileStatus(s, parentPath));
//      }
//      ArrayWritable value = new Har2ArrayWritable(tmpValues.toArray(new Har2FileStatus[0]));
//      dirMap.put(key, value);
//    }
//    dirIndexOut.close();
//    
//    try {
//      dirMap.write(dirMapOut);
//    } catch(Exception e) {
//      LOG.error("Couldn't create directoryMap");
//      e.printStackTrace();
//    } finally {
//      dirMapOut.close();
//    }
//    // touch _HAR2_
//    fs.create(new Path(outPath, "_HAR2_")).close();
      
      super.commitJob(context);
    }
  }
}
