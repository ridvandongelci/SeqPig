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
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import net.sf.samtools.SAMRecord;

import org.apache.hadoop.io.LongWritable;
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
import scala.collection.Iterator;
import scala.runtime.AbstractFunction1;
import fi.tkk.ics.hadoop.bam.SAMInputFormat;
import fi.tkk.ics.hadoop.bam.SAMRecordWritable;

public class SamLoader extends LoadFunc implements LoadMetadata, LoadSparkFunc {
	protected RecordReader in = null;

	private boolean loadAttributes;

	public SamLoader() {
		loadAttributes = false;
		System.out.println("BamLoader: ignoring attributes");
	}

	public SamLoader(String loadAttributesStr) {
		if (loadAttributesStr.equals("yes"))
			loadAttributes = true;
		else {
			loadAttributes = false;
			System.out.println("BamLoader: ignoring attributes");
		}
	}

	@Override
	public Tuple getNext() throws IOException {
		try {

			boolean notDone = in.nextKeyValue();
			if (!notDone) {
				return null;
			}

			SAMRecordWritable currentValue = (SAMRecordWritable) in
					.getCurrentValue();

			return SAMRecordToTuple(currentValue, loadAttributes);
		} catch (InterruptedException e) {
			int errCode = 6018;
			String errMsg = "Error while reading input";
			throw new ExecException(errMsg, errCode,
					PigException.REMOTE_ENVIRONMENT, e);
		}

	}

	/**
	 * @param samrec
	 * @return
	 * @throws IOException
	 * @throws InterruptedException
	 */
	private static Tuple SAMRecordToTuple(SAMRecordWritable currentValue,
			boolean loadAttributes) {
		SAMRecord samrec = currentValue.get();
		ArrayList<Object> mProtoTuple = null;
		if (mProtoTuple == null) {
			mProtoTuple = new ArrayList<Object>();
		}

		mProtoTuple.add(new String(samrec.getReadName()));
		mProtoTuple.add(new Integer(samrec.getAlignmentStart()));
		mProtoTuple.add(new Integer(samrec.getAlignmentEnd()));
		mProtoTuple.add(new String(samrec.getReadString()));
		mProtoTuple.add(new String(samrec.getCigarString()));
		mProtoTuple.add(new String(samrec.getBaseQualityString()));
		mProtoTuple.add(new Integer(samrec.getFlags()));
		mProtoTuple.add(new Integer(samrec.getInferredInsertSize()));
		mProtoTuple.add(new Integer(samrec.getMappingQuality()));
		mProtoTuple.add(new Integer(samrec.getMateAlignmentStart()));
		mProtoTuple.add(new Integer(samrec.getMateReferenceIndex()));
		mProtoTuple.add(new Integer(samrec.getReferenceIndex()));
		mProtoTuple.add(new String(samrec.getReferenceName()));

		if (loadAttributes) {
			Map attributes = new HashMap<String, Object>();

			final List<SAMRecord.SAMTagAndValue> mySAMAttributes = samrec
					.getAttributes();

			for (final SAMRecord.SAMTagAndValue tagAndValue : mySAMAttributes) {

				if (tagAndValue.value != null) {
					if (tagAndValue.value.getClass().getName()
							.equals("java.lang.Character"))
						attributes.put(tagAndValue.tag,
								tagAndValue.value.toString());
					else
						attributes.put(tagAndValue.tag, tagAndValue.value);
				}
			}

			mProtoTuple.add(attributes);
		}

		final TupleFactory mTupleFactory = TupleFactory.getInstance();
		Tuple t = mTupleFactory.newTupleNoCopy(mProtoTuple);
		mProtoTuple = null;
		return t;
	}

	private boolean skipAttributeTag(String tag) {
		return (tag.equalsIgnoreCase("AM") || tag.equalsIgnoreCase("NM")
				|| tag.equalsIgnoreCase("SM") || tag.equalsIgnoreCase("XN")
				|| tag.equalsIgnoreCase("MQ") || tag.equalsIgnoreCase("XT")
				|| tag.equalsIgnoreCase("X0") || tag.equalsIgnoreCase("BQ")
				|| tag.equalsIgnoreCase("X1") || tag.equalsIgnoreCase("XC"));
	}

	@Override
	public InputFormat getInputFormat() {
		return new SAMInputFormat();
	}

	@Override
	public void prepareToRead(RecordReader reader, PigSplit split) {
		in = reader;
	}

	@Override
	public void setLocation(String location, Job job) throws IOException {
		FileInputFormat.setInputPaths(job, location);
	}

	@Override
	public ResourceSchema getSchema(String location, Job job)
			throws IOException {

		Schema s = new Schema();
		s.add(new Schema.FieldSchema("name", DataType.CHARARRAY));
		s.add(new Schema.FieldSchema("start", DataType.INTEGER));
		s.add(new Schema.FieldSchema("end", DataType.INTEGER));
		s.add(new Schema.FieldSchema("read", DataType.CHARARRAY));
		s.add(new Schema.FieldSchema("cigar", DataType.CHARARRAY));
		s.add(new Schema.FieldSchema("basequal", DataType.CHARARRAY));
		s.add(new Schema.FieldSchema("flags", DataType.INTEGER));
		s.add(new Schema.FieldSchema("insertsize", DataType.INTEGER));
		s.add(new Schema.FieldSchema("mapqual", DataType.INTEGER));
		s.add(new Schema.FieldSchema("matestart", DataType.INTEGER));
		s.add(new Schema.FieldSchema("materefindex", DataType.INTEGER));
		s.add(new Schema.FieldSchema("refindex", DataType.INTEGER));
		s.add(new Schema.FieldSchema("refname", DataType.CHARARRAY));
		s.add(new Schema.FieldSchema("attributes", DataType.MAP));
		return new ResourceSchema(s);
	}

	@Override
	public String[] getPartitionKeys(String location, Job job)
			throws IOException {
		return null;
	}

	@Override
	public void setPartitionFilter(Expression partitionFilter)
			throws IOException {
	}

	@Override
	public ResourceStatistics getStatistics(String location, Job job)
			throws IOException {
		return null;
	}

	@Override
	public RDD<Tuple> getRDDfromContext(SparkContext sc, String path,
			JobConf conf) throws IOException {
		// Map key value pairs to tuples by omitting LongWritable
		RDD<Tuple2<LongWritable, SAMRecordWritable>> hadoopRDD = sc
				.newAPIHadoopFile(path, SAMInputFormat.class,
						LongWritable.class, SAMRecordWritable.class, conf);

		ToTupleFunction TO_TUPLE_FUNCTION = new ToTupleFunction(loadAttributes);
		return hadoopRDD.mapPartitions(TO_TUPLE_FUNCTION, true,
				SparkUtil.getManifest(Tuple.class));
	}

	private static class ToTupleFunction extends
			AbstractFunction1<Iterator<Tuple2<LongWritable, SAMRecordWritable>>, Iterator<Tuple>>
			implements Serializable {

		boolean loadAttributes = false;

		public ToTupleFunction(boolean loadAttributes) {
			this.loadAttributes = loadAttributes;
		}

		@Override
		public Iterator<Tuple> apply(
				Iterator<Tuple2<LongWritable, SAMRecordWritable>> input) {
			
			return input.map(new AbstractFunction1<Tuple2<LongWritable,SAMRecordWritable>, Tuple>() {
				@Override
				public Tuple apply(Tuple2<LongWritable, SAMRecordWritable> v1) {
					return SAMRecordToTuple(v1._2(), loadAttributes);
				}
			});
		}

		
	}
}
