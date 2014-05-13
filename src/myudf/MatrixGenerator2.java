package myudf;

import java.io.IOException;

import org.apache.pig.EvalFunc;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;

import fi.aalto.seqpig.ReadRefPositions;

public class MatrixGenerator2 extends EvalFunc<Tuple> {

	
	public Tuple exec(Tuple input) throws IOException,
			org.apache.pig.backend.executionengine.ExecException {
		if (input == null || input.size() == 0)
			return null;
		try {


			int start = ((Integer) input.get(0)).intValue();
			int end = ((Integer) input.get(1)).intValue();
	
			Tuple row = TupleFactory.getInstance().newTuple(end-start+1);
			for(int i=0;i<row.size();i++)
				row.set(i, 0);
			
			DataBag reads = (DataBag)input.get(2);
			
			if(reads!=null)
			{
				ReadRefPositions readRefPositions = new ReadRefPositions();
				for (Tuple read : reads) {
					Tuple r = TupleFactory.getInstance().newTuple(6);
					for(int i =3 ; i<9;i++)
						r.set(i-3, read.get(i));
					DataBag bases = readRefPositions.exec(r);
					for (Tuple base : bases) {
						int pos = ((Integer) base.get(1)).intValue();
						pos = pos - start;
						if(pos>=0 && pos<row.size())
						{
							int count = ((Integer)row.get(pos)).intValue();
							count += 1;
							row.set(pos, count);
						}
					}
				}
			}
			
			return row;

		} catch (Exception e) {
			throw new IOException(
					"Caught exception processing input row ", e);
		}
	}


}
