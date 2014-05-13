package myudf;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.data.BagFactory;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.apache.pig.EvalFunc;
import org.apache.pig.impl.logicalLayer.schema.Schema;
import org.apache.pig.impl.util.WrappedIOException;

import it.crs4.seal.common.AlignOp;
import it.crs4.seal.common.WritableMapping;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class BedSplit extends EvalFunc<DataBag> {
	private BagFactory mBagFactory = BagFactory.getInstance();


	public DataBag exec(Tuple input) throws IOException,
			org.apache.pig.backend.executionengine.ExecException {
		if (input == null || input.size() == 0)
			return null;
		try {
			DataBag output = mBagFactory.newDefaultBag();
			int id = ((Long) input.get(0)).intValue();
			String chrname = (String) input.get(1);
			int start = ((Integer) input.get(2)).intValue();
			int end = ((Integer) input.get(3)).intValue();
			int small = start < end ? start: end;
			int big = start > end ? start: end;
			while(small<=big)
			{
				Tuple tpl = TupleFactory.getInstance().newTuple(3);
				
				tpl.set(0, id);
				tpl.set(1, chrname);
				tpl.set(2, small++);
				
				output.add(tpl);
			}
			
			return output;

		} catch (Exception e) {
			throw WrappedIOException.wrap(
					"Caught exception processing input row ", e);
		}
	}

	public Schema outputSchema(Schema input) {
		try {
			
			Schema bagSchema = new Schema();
			bagSchema.add(new Schema.FieldSchema("id", DataType.INTEGER));
			bagSchema.add(new Schema.FieldSchema("chr", DataType.CHARARRAY));
			bagSchema.add(new Schema.FieldSchema("pos", DataType.INTEGER));
			return new Schema(new Schema.FieldSchema(getSchemaName(this
					.getClass().getName().toLowerCase(), input), bagSchema,
					DataType.BAG));
		} catch (Exception e) {
			return null;
		}
	}
}
