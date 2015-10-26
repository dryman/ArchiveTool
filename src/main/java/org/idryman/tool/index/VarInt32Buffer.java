package org.idryman.tool.index;

import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.ListIterator;

public class VarInt32Buffer implements List<Integer>{
  private ByteBuffer bb;
  private int size=-1;
  
  public VarInt32Buffer(ByteBuffer bb) {
    this.bb = bb;
  }

  public synchronized int size() {
    if (size==-1) {
      int size = 0;
      for (final ByteBuffer bb = this.bb.duplicate(); bb.hasRemaining();) {
        if ((bb.get() & 0x80) == 0)
          size++;
      }
      this.size = size;
    }
    return size;
  }

  public boolean isEmpty() {
    return this.size()==0;
  }

  public boolean contains(Object o) {
    for (Iterator<Integer> iter=this.iterator(); iter.hasNext();) {
      if (iter.next().equals(o)) {
        return true;
      }
    }
    return false;
  }

  public Iterator<Integer> iterator() {
    final ByteBuffer bb = this.bb.duplicate();
    return new Iterator<Integer>() {
      public boolean hasNext() {
        return bb.hasRemaining();
      }
      public Integer next() {
        int val = 0, tmp;
        do {
          tmp = bb.get();
          val = (val << 7) | (tmp & 0x7F);
        } while((tmp & 0x80) != 0);
        return val;
      }
      public void remove() {
        throw new UnsupportedOperationException();
      }
      
    };
  }

  public Object[] toArray() {
 // TODO I still might want to support readInts (instead of read(byte[])) operation
 // int [] is just generally better in performance, right?
 // I can create a special interface for it
    Integer[] arr = new Integer[this.size()];
    Iterator<Integer> iter=this.iterator();
    for (int i=0; iter.hasNext();) {
      arr[i++] = iter.next();
    }
    return arr;
  }

  public <T> T[] toArray(T[] a) {
    throw new UnsupportedOperationException();
  }

  public boolean add(Integer e) {
    throw new UnsupportedOperationException();
  }

  public boolean remove(Object o) {
    throw new UnsupportedOperationException();
  }

  public boolean containsAll(Collection<?> c) {
    throw new UnsupportedOperationException();
  }

  public boolean addAll(Collection<? extends Integer> c) {
    throw new UnsupportedOperationException();
  }

  public boolean addAll(int index, Collection<? extends Integer> c) {
    throw new UnsupportedOperationException();
  }

  public boolean removeAll(Collection<?> c) {
    throw new UnsupportedOperationException();
  }

  public boolean retainAll(Collection<?> c) {
    throw new UnsupportedOperationException();
  }

  public void clear() {
    throw new UnsupportedOperationException();
  }

  public Integer get(int index) {
    Iterator<Integer> iter=this.iterator();
    Integer ret;
    do {
      ret = iter.next();
    } while (index-- > 0);
    return ret;
  }

  public Integer set(int index, Integer element) {
    throw new UnsupportedOperationException();
  }

  public void add(int index, Integer element) {
    throw new UnsupportedOperationException();
  }

  public Integer remove(int index) {
    throw new UnsupportedOperationException();
  }

  public int indexOf(Object o) {
    Iterator<Integer> iter=this.iterator();
    for (int index=0; iter.hasNext(); index++) {
      if (iter.next().equals(o)) {
        return index;
      }
    }
    return -1;
  }

  public int lastIndexOf(Object o) {
    Iterator<Integer> iter=this.iterator();
    int index = -1;
    for (int i=0; iter.hasNext(); i++) {
      if (iter.next().equals(o)) {
        index = i;
      }
    }
    return index;
  }

  public ListIterator<Integer> listIterator() {
    throw new UnsupportedOperationException();
  }

  public ListIterator<Integer> listIterator(int index) {
    throw new UnsupportedOperationException();
  }

  public List<Integer> subList(int fromIndex, int toIndex) {
    throw new UnsupportedOperationException();
  }

}
