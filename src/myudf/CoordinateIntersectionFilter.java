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
import java.util.ArrayList;
import java.util.List;
import java.util.StringTokenizer;

import org.apache.pig.FilterFunc;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.data.Tuple;

// tuple input format:
//   chr
//   position
//   flag
//   mapping quality

public class CoordinateIntersectionFilter extends FilterFunc {

	class RegionEntry {
		public String chrName = "";
		public int start = -1;
		public int end = -1;

		public RegionEntry(String i, int s, int e) {
			chrName = i;
			start = s;
			end = e;
		}

		public String toString() {
			String ret = "region: (" + chrName + "," + start + "," + end + ")";
			return ret;
		}
	}

	protected List<RegionEntry> regions = null;
	protected String regions_str = null;

	public CoordinateIntersectionFilter() {
	}

	public CoordinateIntersectionFilter(String regions_str) {
		this.regions_str = regions_str;
		populateRegions();
	}

	// note: most of this is shamelessly copied from hadoop-bam
	private void populateRegions() {
		StringTokenizer rst = new StringTokenizer(regions_str, ",");
		regions = new ArrayList<RegionEntry>();

		while (rst.hasMoreTokens()) {
			String region = rst.nextToken();

			final StringTokenizer st = new StringTokenizer(region, ":-");
			final String refStr = st.nextToken();
			final int beg, end;

			if (st.hasMoreTokens()) {
				beg = parseCoordinate(st.nextToken());
				end = st.hasMoreTokens() ? parseCoordinate(st.nextToken()) : -1;

				if (beg < 0 || end < 0 || end < beg) {
					continue;
				}
			} else
				beg = end = 0;


			RegionEntry e = new RegionEntry(refStr, beg, end);
			regions.add(e);
		}

		// System.err.println("decoded "+ctr+" regions!");
	}

	private int parseCoordinate(String s) {
		int c;
		try {
			c = Integer.parseInt(s);
		} catch (NumberFormatException e) {
			c = -1;
		}
		if (c < 0)
			System.err.printf(
					"CoordinateFilter :: Not a valid coordinate: '%s'\n", s);
		return c;
	}

	// tuple input format:
	// chrom index
	// start position
	// end postition

	@Override
	public Boolean exec(Tuple input) throws IOException {
		try {
			String chrom = ((String) input.get(0));
			int start_pos = ((Integer) input.get(1)).intValue();
			int end_pos = ((Integer) input.get(2)).intValue();

			for (RegionEntry entry : regions) {
				if (entry.chrName.equalsIgnoreCase(chrom)
						&& ((entry.start <= start_pos && entry.end >= start_pos) || (start_pos <= entry.start && end_pos >= entry.start)))
					return true;
			}

			return false;

		} catch (ExecException ee) {
			throw ee;
		}
	}
}
