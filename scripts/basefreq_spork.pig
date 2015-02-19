set default_parallel 48;
--
-- start of script: count the number of different bases occuring at each reference position
--
--   import BAM file
A = load '$inputfile' using fi.aalto.seqpig.io.BamLoader('yes');
--   following good Pig practice, we try to project early and get rid of fields we do not need
A = FOREACH A GENERATE read, flags, refname, start, cigar, basequal, mapqual;
--   filter out unmapped reads
A = FILTER A BY (flags/4)%2==0;
--   generate reference positions for each read and all its bases
RefPos = FOREACH A GENERATE fi.aalto.seqpig.ReadRefPositions(read, flags, refname, start, cigar, basequal), mapqual;
--   form the union of all of these records
flatset = FOREACH RefPos GENERATE flatten($0), mapqual;
--   group records by refname, position and base
grouped = GROUP flatset BY ($0, $1, $2);
--   count the number of different bases for each position
base_counts = FOREACH grouped GENERATE group.chr, group.pos, group.base, COUNT(flatset);
--   for comparison it is nice to have the output sorted by reference position
base_counts = ORDER base_counts BY chr,pos;
--   store output
store base_counts into '$outputfile';
