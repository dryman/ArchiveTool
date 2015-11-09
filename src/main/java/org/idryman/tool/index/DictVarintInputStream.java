package org.idryman.tool.index;

import java.io.FilterInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.lang.ArrayUtils;

public class DictVarintInputStream extends FilterInputStream{
  private long dict [];
  private UnaryCodeInputStream uis;

  protected DictVarintInputStream(InputStream in) {
    super(in);
    uis = new UnaryCodeInputStream(in);
  }

  public synchronized long readLong() throws IOException {
    if (dict==null) {
      long acc = 0, diff_val;
      int tmp;
      List<Long> list = new ArrayList<>();
      do {
        diff_val = 0;
        do {
          tmp = super.read();
          if ((tmp & 0x80)!=0) {
            diff_val = (diff_val << 6) | (tmp & 0x3F);
            break;
          } else {
            diff_val = (diff_val << 7) | (tmp & 0x7F);
          }
        } while (true);
        acc+=diff_val;
        list.add(acc);
      } while ((tmp & 0x40) == 0);
      dict = ArrayUtils.toPrimitive(list.toArray(new Long[0]));
    }
    final int idx = uis.readInt();
    if (idx == -1) return -1;
    return dict[idx];
  }
}
