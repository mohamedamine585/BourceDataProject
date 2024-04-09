package org.medproject;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Text;

import org.apache.hadoop.io.Writable;

public class PairTextWritable implements Writable {

    private Text _first , _second;

    PairTextWritable(Text fvalue , Text svalue){
        setFirst(fvalue);
        setSecond(svalue);

    }
     
    
    public Text getfirst(){
        return _first;
       }
       public Text getsecond(){
        return _second;
       }
  
    void setFirst(Text value){
        this._first = value;
    }

    void setSecond(Text value){
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
