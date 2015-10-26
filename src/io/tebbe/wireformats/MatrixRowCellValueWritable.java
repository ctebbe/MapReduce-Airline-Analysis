package io.tebbe.wireformats;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Created by ctebbe
 */
public class MatrixRowCellValueWritable implements Writable {

    public final IntWritable row;
    public final IntWritable col;
    public DoubleWritable value;

    public MatrixRowCellValueWritable() {
        row = new IntWritable(0);
        col = new IntWritable(0);
        value = new DoubleWritable(0.0);
    }

    public MatrixRowCellValueWritable(int row, int col, double value) {
        this.row = new IntWritable(row);
        this.col = new IntWritable(col);
        this.value = new DoubleWritable(value);
    }

    public MatrixRowCellValueWritable(MatrixRowCellValueWritable old) {
        this.row = new IntWritable(old.row.get());
        this.col = new IntWritable(old.col.get());
        this.value = new DoubleWritable(old.value.get());
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        row.write(dataOutput);
        col.write(dataOutput);
        value.write(dataOutput);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        row.readFields(dataInput);
        col.readFields(dataInput);
        value.readFields(dataInput);
    }
}
