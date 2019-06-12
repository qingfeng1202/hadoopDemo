package com.hadoop.mapreduce.tq;

import lombok.Data;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

@Data
public class Tq implements WritableComparable<Tq> {

    private int year;

    private int month;

    private int day;

    private int wd;

    @Override
    public int compareTo(Tq o) {
        int compare = Integer.compare(this.getYear(), o.getYear());

        if(compare == 0){
            compare = Integer.compare(this.getMonth(), o.getMonth());

            if(compare == 0){
                compare = Integer.compare(this.getDay(), o.getDay());
            }
        }

        return compare;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeInt(this.getYear());
        dataOutput.writeInt(this.getMonth());
        dataOutput.writeInt(this.getDay());
        dataOutput.writeInt(this.getWd());
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        this.setYear(dataInput.readInt());
        this.setMonth(dataInput.readInt());
        this.setDay(dataInput.readInt());
        this.setWd(dataInput.readInt());
    }

    @Override
    public String toString() {
        return year + "-" + month + "-" + day;
    }
}
