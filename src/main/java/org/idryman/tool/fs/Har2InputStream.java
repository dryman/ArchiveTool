package org.idryman.tool.fs;

import java.io.EOFException;
import java.io.FileNotFoundException;
import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSExceptionMessages;
import org.apache.hadoop.fs.FSInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PositionedReadable;
import org.apache.hadoop.fs.Seekable;
import org.tukaani.xz.SeekableXZInputStream;

import com.google.common.base.Preconditions;

public class Har2InputStream extends FSInputStream implements Seekable, PositionedReadable{
  private static final Log LOG = LogFactory.getLog(Har2InputStream.class);
  private SeekableXZInputStream fis;
  private long                  start;
  private long                  end;

  public Har2InputStream(int block_num) throws FileNotFoundException, IOException {
    /*
     * FIXME should pass in a real FS and identify the file from FS
     * TODO SeekableFileInputStream read from file, but FS here is different..
     */
    
    FileSystem fs = FileSystem.get(new Configuration());
    fis = new SeekableXZInputStream(new SeekableHDFSInputStream(fs, new Path("foo.xz")));
    
    LOG.info("Contains " + fis.getBlockCount() + " blocks");
    start = fis.getBlockPos(block_num);
    end   = start + fis.getBlockSize(block_num);
    LOG.debug("start position at: " + start);
    LOG.debug("end position at: " + end);
    LOG.debug("cursor position at: " + fis.position());
    fis.seek(start);
  }
  
  @Override
  public int read() throws IOException {
    if (fis.position() >= end) {
      return -1;
    }
    return fis.read();
  }
  
  @Override
  public int read(byte buf[], int off, int len) throws IOException {
    LOG.debug("Before read, cursor position at: " + fis.position());
    if (fis.position() >= end) {
      return -1;
    }
    int length = len;
    if (length > end - fis.position()) {
      length = (int) (end - fis.position());
    }
    int value = fis.read(buf, off, length);
    
    LOG.debug("After read, cursor position at: " + fis.position());

    return value;
  }

  public void seek(long position) throws IOException {
    if (position < 0) {
      throw new EOFException(FSExceptionMessages.NEGATIVE_SEEK);
    }
    if (position + start > end) {
      throw new EOFException(FSExceptionMessages.CANNOT_SEEK_PAST_EOF);
    }
    fis.seek(position + start);
  }



  public long getPos() throws IOException {
    return fis.position()-start;
  }



  public boolean seekToNewSource(long targetPos) throws IOException {
    return false;
  }

  public void close() throws IOException {
    fis.close();
    LOG.debug("XZInputStream closed");
  }
  
}
