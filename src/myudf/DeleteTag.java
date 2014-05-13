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

package myudf;

import java.io.IOException;
import java.util.Map;

import org.apache.pig.EvalFunc;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.logicalLayer.schema.Schema;
import org.apache.pig.data.DataType;

public class DeleteTag extends EvalFunc<Map<String, Object>>{

	@Override
	public Map<String, Object> exec(Tuple input) throws IOException {
		
		//Input should have 2 fields
		if (input == null || input.size() == 0)
			return null;
		
		//Get maps from tuple
		Map<String,Object> tags = (Map<String,Object>) input.get(0);
		Tuple deleteTags = (Tuple) input.get(1);
		
		//If one of the maps is not there create an empty map
		if(tags == null)
		{
			return null;
		}
		
		if(deleteTags != null)
		{
			for (Object tagToDelete : deleteTags)
			{
				tags.remove(tagToDelete);
			}
		}
		return tags;
	}
	
	public Schema outputSchema(Schema input) {
        try{
            Schema tupleSchema = new Schema();
			tupleSchema.add(new Schema.FieldSchema("attributes", DataType.MAP));
            return tupleSchema;
        }catch (Exception e){
                return null;
        }
    }

}
