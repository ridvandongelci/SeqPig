// Copyright (c) 2012 Aalto University
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to
// deal in the Software without restriction, including without limitation the
// rights to use, copy, modify, merge, publish, distribute, sublicense, and/or
// sell copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in
// all copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING
// FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS
// IN THE SOFTWARE.

package fi.aalto.seqpig.io;

import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.pig.Expression;
import org.apache.pig.LoadFunc;
import org.apache.pig.LoadMetadata;
import org.apache.pig.PigException;
import org.apache.pig.ResourceSchema;
import org.apache.pig.ResourceStatistics;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.PigSplit;
import org.apache.pig.backend.hadoop.executionengine.spark.SparkUtil;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.apache.pig.impl.logicalLayer.schema.Schema;
import org.apache.pig.spark.LoadSparkFunc;
import org.apache.spark.SparkContext;
import org.apache.spark.rdd.RDD;

import scala.Tuple2;
import scala.runtime.AbstractFunction1;
import fi.tkk.ics.hadoop.bam.FastqInputFormat;
import fi.tkk.ics.hadoop.bam.FastqInputFormat.FastqRecordReader;
import fi.tkk.ics.hadoop.bam.SequencedFragment;

public class FastqLoader extends LoadFunc implements LoadMetadata, LoadSparkFunc {
    protected RecordReader in = null;
  
    private static TupleFactory mTupleFactory = TupleFactory.getInstance();

    // tuple format:
    //
    //   instrument:string
    //   run_number:int
    //   flow_cell_id: string
    //   lane: int
    //   tile: int
    //   xpos: int
    //   ypos: int
    //   read: int
    //   qc_passed: boolean
    //   control_number: int
    //   index_sequence: string
    //   sequence: string
    //   quality: string (note: we assume that encoding chosen on command line!!!)
    
    public FastqLoader() {}

    @Override
    public Tuple getNext() throws IOException {
        try {
	    
	   
	    
            boolean notDone = in.nextKeyValue();
            if (!notDone) {
                return null;
            }

	    Text fastqrec_name = ((FastqRecordReader)in).getCurrentKey();
            SequencedFragment fastqrec = ((FastqRecordReader)in).getCurrentValue();
	   
	    //mProtoTuple.add(new String(fastqrec_name.toString()));
	    //Refactoring for reusability
	    return SequencedFragmentToTuple(fastqrec);
        } catch (InterruptedException e) {
            int errCode = 6018;
            String errMsg = "Error while reading Fastq input: check data format! (Casava 1.8?)";
            throw new ExecException(errMsg, errCode,
				    PigException.REMOTE_ENVIRONMENT, e);
        }

    }

	private static Tuple SequencedFragmentToTuple(SequencedFragment fastqrec) {

		ArrayList<Object> mProtoTuple = null;
		if (mProtoTuple == null) {
			mProtoTuple = new ArrayList<Object>();
		}
		mProtoTuple.add(fastqrec.getInstrument());
		mProtoTuple.add(fastqrec.getRunNumber());
		mProtoTuple.add(fastqrec.getFlowcellId());
		mProtoTuple.add(fastqrec.getLane());
		mProtoTuple.add(fastqrec.getTile());
		mProtoTuple.add(fastqrec.getXpos());
		mProtoTuple.add(fastqrec.getYpos());
		mProtoTuple.add(fastqrec.getRead());
		mProtoTuple.add(fastqrec.getFilterPassed());
		mProtoTuple.add(fastqrec.getControlNumber());
		mProtoTuple.add(fastqrec.getIndexSequence());
		mProtoTuple.add(fastqrec.getSequence().toString());
		mProtoTuple.add(fastqrec.getQuality().toString());

		Tuple t = mTupleFactory.newTupleNoCopy(mProtoTuple);
		mProtoTuple = null;
		return t;
	}

    @Override
    public InputFormat getInputFormat() {
        return new FastqInputFormat();
    }

    @Override
    public void prepareToRead(RecordReader reader, PigSplit split) {
        in = reader;
    }

    @Override
    public void setLocation(String location, Job job)
	throws IOException {
        FileInputFormat.setInputPaths(job, location);
    }

    @Override
    public ResourceSchema getSchema(String location, Job job) throws IOException {
       
	Schema s = new Schema();
	s.add(new Schema.FieldSchema("instrument", DataType.CHARARRAY));
	s.add(new Schema.FieldSchema("run_number", DataType.INTEGER));
	s.add(new Schema.FieldSchema("flow_cell_id", DataType.CHARARRAY));
	s.add(new Schema.FieldSchema("lane", DataType.INTEGER));	
	s.add(new Schema.FieldSchema("tile", DataType.INTEGER));
	s.add(new Schema.FieldSchema("xpos", DataType.INTEGER));
	s.add(new Schema.FieldSchema("ypos", DataType.INTEGER));
	s.add(new Schema.FieldSchema("read", DataType.INTEGER));
	s.add(new Schema.FieldSchema("qc_passed", DataType.BOOLEAN));
	s.add(new Schema.FieldSchema("control_number", DataType.INTEGER));
	s.add(new Schema.FieldSchema("index_sequence", DataType.CHARARRAY));
	s.add(new Schema.FieldSchema("sequence", DataType.CHARARRAY));
	s.add(new Schema.FieldSchema("quality", DataType.CHARARRAY));

        return new ResourceSchema(s);
    }

    @Override
    public String[] getPartitionKeys(String location, Job job) throws IOException { return null; }

    @Override
    public void setPartitionFilter(Expression partitionFilter) throws IOException { }

    @Override
    public ResourceStatistics getStatistics(String location, Job job) throws IOException { return null; }

    /*
     * (non-Javadoc)
     * @see org.apache.pig.spark.LoadSparkFunc#getRDDfromContext(org.apache.spark.SparkContext, java.lang.String, org.apache.hadoop.mapred.JobConf)
     * LoadSparkFunc implementation ignores Text and converts SequencedFragment to Tuple
     */
    @Override
	public RDD<Tuple> getRDDfromContext(SparkContext sc, String path,
			JobConf conf) {
		RDD<Tuple2<Text, SequencedFragment>> hadoopRDD = sc
				.newAPIHadoopFile(path, FastqInputFormat.class,
						Text.class, SequencedFragment.class, conf);
		
		ToTupleFunction TO_TUPLE_FUNCTION = new ToTupleFunction();
		return hadoopRDD.map(TO_TUPLE_FUNCTION,
				SparkUtil.getManifest(Tuple.class));
	}
	
	private static class ToTupleFunction extends
			AbstractFunction1<Tuple2<Text, SequencedFragment>, Tuple>
			implements Serializable {

		@Override
		public Tuple apply(Tuple2<Text, SequencedFragment> v1) {
			return SequencedFragmentToTuple(v1._2());
		}
	}
}
