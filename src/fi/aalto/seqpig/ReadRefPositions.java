package fi.aalto.seqpig;

import java.io.IOException;
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

public class ReadRefPositions extends EvalFunc<DataBag> {
	private WritableMapping mapping = new WritableMapping();
	private ArrayList<Integer> refPositions = new ArrayList(150);
	private TupleFactory mTupleFactory = TupleFactory.getInstance();
	private BagFactory mBagFactory = BagFactory.getInstance();

	// tuple input format:
	// sequence
	// flag
	// chr
	// position
	// cigar
	// base qualities

	// tuple output format:
	// chr
	// position
	// base
	// base quality
	private void loadTuple(Tuple tpl)
			throws org.apache.pig.backend.executionengine.ExecException {
		mapping.clear();

		mapping.setSequence((String) tpl.get(0));
		mapping.setFlag(((Integer) tpl.get(1)).intValue());

		// now get the rest of the fields
		if (mapping.isMapped()) {
			mapping.setContig((String) tpl.get(2));
			mapping.set5Position(((Integer) tpl.get(3)).intValue());
			mapping.setAlignment(AlignOp.scanCigar((String) tpl.get(4)));
		}
	}

	public DataBag exec(Tuple input) throws IOException,
			org.apache.pig.backend.executionengine.ExecException {
		if (input == null || input.size() == 0)
			return null;
		try {
			DataBag output = mBagFactory.newDefaultBag();
			loadTuple(input);

			String seq = mapping.getSequenceString();
			String basequal = (String) input.get(5);

			//base quality or sequence can be unstored.
			boolean seqStored = !seq.equals("*");
			boolean basequalStored = !basequal.equals("*");

			try {
				mapping.calculateReferenceCoordinates(refPositions);
			} catch (RuntimeException e) {
				if(!seq.equals("*"))
					throw e;
			}

			for (int i = 0; i < refPositions.size(); ++i) {
				int pos = refPositions.get(i);
				if (pos >= 0) {
					Tuple tpl = TupleFactory.getInstance().newTuple(4);
					tpl.set(0, mapping.getContig());
					tpl.set(1, pos);
					// if sequence is not stored put star ("*") as base
					tpl.set(2, seqStored ? seq.substring(i, i + 1) : "*");
					// if base quality is not stored put star ("*") as base
					// quality
					tpl.set(3, basequalStored ? basequal.substring(i, i + 1)
							: "*");
					output.add(tpl);
				}
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
			bagSchema.add(new Schema.FieldSchema("chr", DataType.CHARARRAY));
			bagSchema.add(new Schema.FieldSchema("pos", DataType.INTEGER));
			bagSchema.add(new Schema.FieldSchema("base", DataType.CHARARRAY));
			bagSchema
					.add(new Schema.FieldSchema("basequal", DataType.CHARARRAY));

			return new Schema(new Schema.FieldSchema(getSchemaName(this
					.getClass().getName().toLowerCase(), input), bagSchema,
					DataType.BAG));
		} catch (Exception e) {
			return null;
		}
	}
}
