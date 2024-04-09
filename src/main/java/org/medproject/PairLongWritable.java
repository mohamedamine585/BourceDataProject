package org.medproject;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.LongWritable;

import org.apache.hadoop.io.Writable;

public class PairLongWritable implements Writable {

    private  LongWritable _first , _second;
     
    
    public LongWritable getfirst(){
        return _first;
       }
       public LongWritable getsecond(){
        return _second;
       }
  
    void setFirst(LongWritable value){
        this._first = value;
    }

    void setSecond(LongWritable value){
        this._second = value;
    }
    
    
    @Override
    public void write(DataOutput out) throws IOException {
       _first.write(out);
        _second.write(out);
    }
    @Override
    public void readFields(DataInput in) throws IOException {
      _first.readFields(in);
      _second.readFields(in);
    }
    
    
}
