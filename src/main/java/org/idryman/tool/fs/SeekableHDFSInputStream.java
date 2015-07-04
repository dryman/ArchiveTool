package org.idryman.tool.fs;

import java.io.IOException;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.tukaani.xz.SeekableInputStream;

import com.google.common.base.Preconditions;

public class SeekableHDFSInputStream extends SeekableInputStream{
  FSDataInputStream stream;
  long length;
  
  public SeekableHDFSInputStream(FileSystem fs, Path path) throws IOException {
    Preconditions.checkArgument(fs.exists(path));
    Preconditions.checkArgument(fs.isFile(path));
    stream = fs.open(path);
    length = fs.getFileStatus(path).getLen();
  }

  @Override
  public long length() throws IOException {
    return length;
  }

  @Override
  public long position() throws IOException {
    return stream.getPos();
  }

  @Override
  public void seek(long pos) throws IOException {
    stream.seek(pos);
  }

  @Override
  public int read() throws IOException {
    return stream.read();
  }
}
