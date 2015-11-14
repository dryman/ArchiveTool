package org.idryman.tool.index;

import java.io.BufferedReader;
import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;

import org.apache.log4j.BasicConfigurator;
import org.junit.Before;
import org.junit.Test;

public class Pfor384DictVarintOutputStreamTest {
  
  @Before
  public void setup(){
    BasicConfigurator.configure();
  }

  @Test
  public void test() throws IOException {
    InputStream is = Prototype.class.getResourceAsStream("integers");
    BufferedReader br = new BufferedReader(new InputStreamReader(is));
    String line;
    
    ByteArrayOutputStream bos1 = new ByteArrayOutputStream();
    DataOutputStream dos = new DataOutputStream(bos1);
    Pfor384DictVarintOutputStream pdvos = new Pfor384DictVarintOutputStream(dos);
    
    ArrayList<Long> lst = new ArrayList<>();
    
    while((line=br.readLine())!=null) {
      pdvos.writeLong(Long.parseLong(line));
      //pdvos.writeLong(Long.parseLong(line));
    }
    pdvos.close();
    
    
    
    //pdvos.
  }

}
