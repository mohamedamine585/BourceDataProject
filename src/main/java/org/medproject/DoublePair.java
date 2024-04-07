package org.medproject;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;

/**
 * IntPair
 */
public class DoublePair implements Writable{
  private double first;
  private double second ;
  public DoublePair(){
    set(0,0);
  }
     public void set(double first , double second){
        this.first = first;
        this.second = second;
     }

     public double getfirst(){
      return first;
     }
     public double getsecond(){
      return second;
     }

    @Override
    public void write(DataOutput out) throws IOException {
       out.writeDouble(first);
       out.writeDouble(second);
    }
    @Override
    public void readFields(DataInput in) throws IOException {
      first = in.readDouble();
      second = in.readDouble();
    }
}