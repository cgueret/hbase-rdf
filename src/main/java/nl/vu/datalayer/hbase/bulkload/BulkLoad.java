package nl.vu.datalayer.hbase.bulkload;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Iterator;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;

import nl.vu.datalayer.hbase.connection.HBaseConnection;
import nl.vu.datalayer.hbase.connection.NativeJavaConnection;
import nl.vu.datalayer.hbase.id.BaseId;
import nl.vu.datalayer.hbase.id.DataPair;
import nl.vu.datalayer.hbase.id.TypedId;
import nl.vu.datalayer.hbase.id.HBaseValue;
import nl.vu.datalayer.hbase.schema.HBPrefixMatchSchema;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.HFileOutputFormat;
import org.apache.hadoop.hbase.mapreduce.LoadIncrementalHFiles;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.CounterGroup;
import org.apache.hadoop.mapreduce.Counters;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;

/**
 * Class representing main entry point for Bulk loading process
 * 
 */

public class BulkLoad {
	
	/**
	 * Used to load successively the tables containing quads
	 */
	private static HTableInterface currentTable = null;
	
	private static HTableInterface string2Id = null;
	private static HTableInterface id2String = null;
	
	/**
	 * Cluster parameters used to estimate number of reducers 
	 */
	public static int CLUSTER_SIZE = 32;
	public static int TASK_PER_NODE = 2;
	
	
	/**
	 * Estimate of a quad size  
	 */
	public static int QUAD_AVERAGE_SIZE = 230;
	public static int ELEMENTS_PER_QUAD = 4;
	
	
	/**
	 * Estimate of imbalance of data  distributed across reducers
	 * 1.0 = data equally distributed across reducers 
	 */
	public static double LOAD_BALANCER_FACTOR = 1.2;
	
	//public static SortedMap<Short, Long> sufixCounters;
	
	public static long totalStringCount, numericalCount, literalCount, bNodeCount;
	public static int tripleToResourceReduceTasks;
	
	public static String schemaSuffix = "";

	private static Path input;

	private static int inputEstimateSize;

	private static String outputPath;

	private static boolean onlyTriples;

	private static NativeJavaConnection con;
	
	/*public static void main(String[] args) {
		try {
		Path input = new Path(args[0]);
		String outputPath = args[2];
		Path resourceIds = new Path(outputPath+TripleToResource.RESOURCE_IDS_DIR);
		Path convertedTripletsPath = new Path(outputPath+ResourceToTriple.TEMP_TRIPLETS_DIR);
		
		Path string2IdInput = new Path(outputPath+TripleToResource.ID2STRING_DIR);
		Path string2IdOutput = new Path(outputPath+StringIdAssoc.STRING2ID_DIR);
		
		HBaseConnection con = new HBaseConnection(HBaseConnection.JAVA_API);
		HBPrefixMatchSchema.createCounterTable(con.getAdmin());
		
		long start = System.currentTimeMillis();
		
		Job j1 = createTripleToResourceJob(input, resourceIds, Integer.parseInt(args[1]));
		j1.getConfiguration().set("outputPath", outputPath);
		j1.waitForCompletion(true);
		
		long firstJob = System.currentTimeMillis()-start;
		System.out.println("First pass finished in "+firstJob+" ms");
		
		//retrieve counter values and build a SortedMap
		retrieveCounters(j1);
		long startPartition = HBPrefixMatchSchema.updateLastCounter(tripleToResourceReduceTasks, con.getConfiguration())+1;
		}
		catch (Exception e) {
			e.printStackTrace();
		}
	}*/
	
	/**
	 * @param args
	 */
	public static void main(String[] args) {
		try {
			if (args.length != 5){
				System.out.println("Usage: bulkLoad <inputPath> <inputSizeEstimate in MB> <outputPath> <schemaSuffix> <onlyTriples(true/false)>");
				return;
			}		
			initializeLocalVariables(args); 
			
			Path convertedTripletsPath = new Path(outputPath+ResourceToTriple.TEMP_TRIPLETS_DIR);
			Path idStringAssocInput = new Path(outputPath+"/"+TripleToResource.ID2STRING_DIR);
			
			con = (NativeJavaConnection)HBaseConnection.create(HBaseConnection.NATIVE_JAVA);
			HBPrefixMatchSchema prefMatchSchema = new HBPrefixMatchSchema(con, schemaSuffix);
			prefMatchSchema.createCounterTable(con.getAdmin());
			
			long startPartition = runIdGenerationJobs(convertedTripletsPath, idStringAssocInput);
		
			//create all tables containing PrefixMatch schema ----------------------
			System.out.println(totalStringCount+" : "+numericalCount);
					
			prefMatchSchema.setTableSplitInfo(totalStringCount, numericalCount, 
					tripleToResourceReduceTasks, startPartition, onlyTriples);
			prefMatchSchema.create();
			
			LoadIncrementalHFiles bulkLoad = new LoadIncrementalHFiles(con.getConfiguration());
			
			bulkLoadIdStringMappingTables(idStringAssocInput, bulkLoad);		
			bulkLoadQuadTables(convertedTripletsPath, bulkLoad);
			
			con.close();		
		} catch (IOException e) {
			e.printStackTrace();
		}
		catch(Exception e){
			e.printStackTrace();
		}
	}

	private static void bulkLoadQuadTables(Path convertedTripletsPath, LoadIncrementalHFiles bulkLoad) throws Exception, IOException, InterruptedException, ClassNotFoundException {
		Path spocPath = new Path(outputPath+"/"+HBPrefixMatchSchema.TABLE_NAMES[HBPrefixMatchSchema.SPOC]);
		Path pocsPath = new Path(outputPath+"/"+HBPrefixMatchSchema.TABLE_NAMES[HBPrefixMatchSchema.POCS]);
		Path ospcPath = new Path(outputPath+"/"+HBPrefixMatchSchema.TABLE_NAMES[HBPrefixMatchSchema.OSPC]);
		
		//SPOC--------------------
		Job j5 = createPrefixMatchJob(con, convertedTripletsPath, spocPath, HBPrefixMatchSchema.SPOC, PrefixMatch.PrefixMatchSPOCMapper.class);
		j5.waitForCompletion(true);
		
		doBulkLoad(bulkLoad, spocPath, currentTable, con);

		if (onlyTriples == false){
			bulkLoadQuadOnlyTables(convertedTripletsPath, bulkLoad);
		}
		
		//OSPC---------------------------
		Job j10 = createPrefixMatchJob(con, convertedTripletsPath, ospcPath, HBPrefixMatchSchema.OSPC, PrefixMatch.PrefixMatchOSPCMapper.class);
		j10.waitForCompletion(true);
		
		doBulkLoad(bulkLoad, ospcPath, currentTable, con);
		
		//POCS---------------------
		Job j6 = createPrefixMatchJob(con, convertedTripletsPath, pocsPath, HBPrefixMatchSchema.POCS, PrefixMatch.PrefixMatchPOCSMapper.class);
		j6.waitForCompletion(true);
		
		doBulkLoad(bulkLoad, pocsPath, currentTable, con);
	}

	private static void bulkLoadQuadOnlyTables(Path convertedTripletsPath, LoadIncrementalHFiles bulkLoad) throws Exception, IOException, InterruptedException, ClassNotFoundException {
		Path cspoPath = new Path(outputPath+"/"+HBPrefixMatchSchema.TABLE_NAMES[HBPrefixMatchSchema.CSPO]);
		Path cpsoPath = new Path(outputPath+"/"+HBPrefixMatchSchema.TABLE_NAMES[HBPrefixMatchSchema.CPSO]);
		Path ocspPath = new Path(outputPath+"/"+HBPrefixMatchSchema.TABLE_NAMES[HBPrefixMatchSchema.OCSP]);
		
		//CSPO----------------------
		Job j7 = createPrefixMatchJob(con, convertedTripletsPath, cspoPath, HBPrefixMatchSchema.CSPO, PrefixMatch.PrefixMatchCSPOMapper.class);
		j7.waitForCompletion(true);
		
		doBulkLoad(bulkLoad, cspoPath, currentTable, con);
		
		//CPSO-------------------------
		Job j8 = createPrefixMatchJob(con, convertedTripletsPath, cpsoPath, HBPrefixMatchSchema.CPSO, PrefixMatch.PrefixMatchCPSOMapper.class);
		j8.waitForCompletion(true);
		
		doBulkLoad(bulkLoad, cpsoPath, currentTable, con);
		
		//OCSP--------------------------			
		Job j9 = createPrefixMatchJob(con, convertedTripletsPath, ocspPath, HBPrefixMatchSchema.OCSP, PrefixMatch.PrefixMatchOCSPMapper.class);
		j9.waitForCompletion(true);
		
		doBulkLoad(bulkLoad, ocspPath, currentTable, con);
	}

	final private static void bulkLoadIdStringMappingTables(Path idStringAssocInput, LoadIncrementalHFiles bulkLoad) throws Exception, IOException, InterruptedException, ClassNotFoundException {
		Path string2IdOutput = new Path(outputPath+StringIdAssoc.STRING2ID_DIR);
		Path id2StringOutput = new Path(outputPath+StringIdAssoc.ID2STRING_DIR);
		
		//String2Id PASS----------------------------------------------		
		Job j3 = createString2IdJob(con, idStringAssocInput, string2IdOutput);
		j3.waitForCompletion(true);
		
		//string2Id = con.getTable(HBPrefixMatchSchema.STRING2ID+schemaSuffix);
		doBulkLoad(bulkLoad, string2IdOutput, string2Id, con);
		
		System.out.println("Finished bulk load for String2Id table");//=====================//=============
		
		Job j4 = createId2StringJob(con, idStringAssocInput, id2StringOutput);
		j4.waitForCompletion(true);
		
		doBulkLoad(bulkLoad, id2StringOutput, id2String, con);		
		System.out.println("Finished bulk load for Id2String table");//=====================//==================================
	}

	final private static long runIdGenerationJobs(Path convertedTripletsPath, Path idStringAssocInput) throws IOException, InterruptedException, ClassNotFoundException {		
		Path resourceIds = new Path(outputPath+"/"+TripleToResource.RESOURCE_IDS_DIR);
		
		runTripleToResourceJob(idStringAssocInput, resourceIds);
		long startPartition = HBPrefixMatchSchema.updateLastCounter(tripleToResourceReduceTasks, con.getConfiguration(), schemaSuffix)+1;
		//long startPartition = HBPrefixMatchSchema.getLastCounter(con.getConfiguration())+1;
		
		//SECOND PASS-----------------------------------------------	
		Job j2 = createResourceToTripleJob(resourceIds, convertedTripletsPath);	
		j2.waitForCompletion(true);
		System.out.println("Second pass finished");
		
		//read counters from file
		//buildCountersFromFile();
		
		return startPartition;
	}

	private static void runTripleToResourceJob(Path idStringAssocInput, Path resourceIds) throws IOException, InterruptedException, ClassNotFoundException {
		long start = System.currentTimeMillis();
		Job j1 = createTripleToResourceJob(input, resourceIds, inputEstimateSize);
		j1.getConfiguration().set("outputPath", outputPath);
		j1.getConfiguration().set("schemaSuffix", schemaSuffix);
		j1.waitForCompletion(true);
		
		//move side effect files out of TripleToResource output directory
		moveIdStringAssocDirectory(resourceIds, idStringAssocInput);
		
		long firstJob = System.currentTimeMillis()-start;
		System.out.println("First pass finished in "+firstJob+" ms");
		
		retrieveTripleToResourceCounters(j1);
	}

	private static void initializeLocalVariables(String[] args) {
		input = new Path(args[0]);
		inputEstimateSize = Integer.parseInt(args[1]);
		outputPath = args[2];
		schemaSuffix = args[3];
		onlyTriples = Boolean.parseBoolean(args[4]);
	}


	public static Job createTripleToResourceJob(Path input, Path output, int inputSizeEstimate) throws IOException{
		Configuration conf = new Configuration();
		conf.setInt("mapred.job.reuse.jvm.num.tasks", -1);
		
		//the intermediate keys are larger than intermediate values
		conf.setFloat("io.sort.record.percent", 0.6f);
		
		//for large data we will spill anyway, so might as well start it as soon as possible, 
		//so that the mapper doesn't block
		conf.setFloat("io.sort.spill.percent", 0.6f);
		
		Job j = new Job(conf);
		
		j.setJobName("TripleToResource");
		
		long numInputBytes = (long)(inputSizeEstimate*Math.pow(10.0, 6.0));
		long numQuads = numInputBytes/QUAD_AVERAGE_SIZE;
		long totalNumberOfElements = numQuads*ELEMENTS_PER_QUAD;		
		//tripleToResourceReduceTasks = (int)(Math.ceil((double)totalNumberOfElements/Math.pow(2.0, 24.0)) * LOAD_BALANCER_FACTOR);
		tripleToResourceReduceTasks = 2;//TODO
		System.out.println("Number of reduce tasks: "+tripleToResourceReduceTasks);
		
		j.setJarByClass(BulkLoad.class);
		j.setMapperClass(TripleToResource.TripleToResourceMapper.class);
		j.setReducerClass(TripleToResource.TripleToResourceReducer.class);
		
		j.setOutputKeyClass(TypedId.class);
		j.setOutputValueClass(DataPair.class);
		
		j.setMapOutputKeyClass(HBaseValue.class);
		j.setMapOutputValueClass(DataPair.class);

		j.setInputFormatClass(TextInputFormat.class);
		j.setOutputFormatClass(SequenceFileOutputFormat.class);
		TextInputFormat.setInputPaths(j, input);
		SequenceFileOutputFormat.setOutputPath(j, output);
		
		j.setNumReduceTasks(tripleToResourceReduceTasks);
		
		return j;
	}
	
	public static Job createResourceToTripleJob(Path input, Path output) throws IOException {

		Configuration conf = new Configuration();
		conf.setInt("mapred.job.reuse.jvm.num.tasks", -1);
		conf.setFloat("io.sort.record.percent", 0.2f);
		conf.setFloat("io.sort.spill.percent", 0.7f);
		
		Job j = new Job();
		j.setJobName("ResourceToTriple");
		
		int reduceTasks = (int)(1.75*(double)CLUSTER_SIZE*(double)TASK_PER_NODE);
		j.setNumReduceTasks(reduceTasks);

		j.setJarByClass(BulkLoad.class);
		j.setMapperClass(ResourceToTriple.ResourceToTripleMapper.class);
		j.setReducerClass(ResourceToTriple.ResourceToTripleReducer.class);

		j.setMapOutputKeyClass(BaseId.class);
		j.setMapOutputValueClass(DataPair.class);
		j.setOutputKeyClass(NullWritable.class);
		j.setOutputValueClass(ImmutableBytesWritable.class);
		j.setInputFormatClass(SequenceFileInputFormat.class);
		j.setOutputFormatClass(SequenceFileOutputFormat.class);
		SequenceFileInputFormat.setInputPaths(j, input);
		SequenceFileOutputFormat.setOutputPath(j, output);

		return j;
	}
	
	public static Job createString2IdJob(HBaseConnection con, Path input, Path output) throws Exception {

		JobConf conf = new JobConf();
		conf.setInt("mapred.job.reuse.jvm.num.tasks", -1);
		conf.setFloat("io.sort.record.percent", 0.6f);//intermediate keys are usually bigger than intermediate values 
		conf.setFloat("io.sort.spill.percent", 0.6f);
		Job j = new Job(conf);

		j.setJobName(HBPrefixMatchSchema.STRING2ID+schemaSuffix);
		j.setJarByClass(BulkLoad.class);
		j.setMapperClass(StringIdAssoc.String2IdMapper.class);
		j.setMapOutputKeyClass(ImmutableBytesWritable.class);
		j.setMapOutputValueClass(Put.class);
		j.setInputFormatClass(SequenceFileInputFormat.class);
		j.setOutputFormatClass(HFileOutputFormat.class);

		SequenceFileInputFormat.setInputPaths(j, input);
		HFileOutputFormat.setOutputPath(j, output);

		string2Id = con.getTable(HBPrefixMatchSchema.STRING2ID+schemaSuffix);

		HFileOutputFormat.configureIncrementalLoad(j, (HTable)string2Id);
		return j;
	}
	
	public static Job createId2StringJob(HBaseConnection con, Path input, Path output) throws Exception {

		Job j = new Job();

		j.setJobName(HBPrefixMatchSchema.ID2STRING+schemaSuffix);
		j.setJarByClass(BulkLoad.class);
		j.setMapperClass(StringIdAssoc.Id2StringMapper.class);
		j.setMapOutputKeyClass(ImmutableBytesWritable.class);
		j.setMapOutputValueClass(Put.class);
		j.setInputFormatClass(SequenceFileInputFormat.class);
		j.setOutputFormatClass(HFileOutputFormat.class);

		SequenceFileInputFormat.setInputPaths(j, input);
		HFileOutputFormat.setOutputPath(j, output);

		id2String = con.getTable(HBPrefixMatchSchema.ID2STRING+schemaSuffix);

		HFileOutputFormat.configureIncrementalLoad(j, (HTable)id2String);

		return j;
	}
	
	public static Job createPrefixMatchJob(HBaseConnection con, Path input, Path output, int tableIndex, Class<? extends Mapper> cls) throws Exception {		
		Configuration conf = new Configuration();
		conf.setFloat("io.sort.record.percent", 0.3f);
		Job j = new Job(conf);

		j.setJobName(HBPrefixMatchSchema.TABLE_NAMES[tableIndex]+schemaSuffix);
		j.setJarByClass(BulkLoad.class);
		j.setMapperClass(cls);
		j.setMapOutputKeyClass(ImmutableBytesWritable.class);
		j.setMapOutputValueClass(Put.class);
		j.setInputFormatClass(SequenceFileInputFormat.class);
		j.setOutputFormatClass(HFileOutputFormat.class);

		SequenceFileInputFormat.setInputPaths(j, input);
		HFileOutputFormat.setOutputPath(j, output);

		currentTable = con.getTable(HBPrefixMatchSchema.TABLE_NAMES[tableIndex]+schemaSuffix);

		HFileOutputFormat.configureIncrementalLoad(j, (HTable)currentTable);

		return j;
	}
	
	public static void retrieveTripleToResourceCounters(Job j1) throws IOException{
		//sufixCounters = new TreeMap<Short, Long>();
		
		Counters counters = j1.getCounters();
		CounterGroup numGroup = counters.getGroup(TripleToResource.TripleToResourceReducer.NUMERICAL_GROUP);
		totalStringCount = numGroup.findCounter("NonNumericals").getValue();
		numericalCount = numGroup.findCounter("Numericals").getValue();
		
		CounterGroup elemsGroup = counters.getGroup(TripleToResource.TripleToResourceReducer.ELEMENT_TYPE_GROUP);
		literalCount = elemsGroup.findCounter("Literals").getValue();
		bNodeCount = elemsGroup.findCounter("Blanks").getValue();
		
		//build histogram
		/*CounterGroup histogramGroup = counters.getGroup(TripleToResource.TripleToResourceReducer.HISTOGRAM_GROUP);
		Iterator<Counter> it = histogramGroup.iterator();
		while (it.hasNext()){
			Counter c = it.next();
			short b = Short.parseShort(c.getName(), 16);
			//System.out.println((char)b +" "+c.getName()+" "+c.getValue());
			
			long half = c.getValue()/2;
			sufixCounters.put(b, half);
			sufixCounters.put((short)(b | 0x01), c.getValue()-half);
		}*/		
		
		//save counter values to file
		FileWriter file = new FileWriter("Counters");
		file.write(Long.toString(totalStringCount)+"\n");
		file.write(Long.toString(numericalCount)+"\n");
		file.write(Integer.toString(tripleToResourceReduceTasks)+"\n");
		
		/*for (Map.Entry<Short, Long> entry : sufixCounters.entrySet()) {
			file.write(entry.getKey()+"@"+entry.getValue()+"\n");
		}*/
		file.close();
	}
	
	public static void buildCountersFromFile() throws IOException{
		//sufixCounters = new TreeMap();
		FileReader file2 = new FileReader("Counters");
		BufferedReader reader = new BufferedReader(file2);
		totalStringCount = Long.parseLong(reader.readLine());
		numericalCount = Long.parseLong(reader.readLine());
		tripleToResourceReduceTasks = Integer.parseInt(reader.readLine());
		
		/*String line;
		long totalCounter = 0;
		while ((line=reader.readLine()) != null){
			String []tokens  = line.split("@");
			short s = Short.parseShort(tokens[0]);
			long current = Long.parseLong(tokens[1]);
			System.out.println("Short: "+s+"; Character: "+(char)s+"; "+current);
			sufixCounters.put(s, current);
			totalCounter += current;
		}
		System.out.println(totalCounter);*/
	}

	private static void doBulkLoad(LoadIncrementalHFiles bulkLoad, Path dir, HTableInterface table, NativeJavaConnection con) throws InterruptedException{
		boolean timedOut = false;
		try{
			long start = System.currentTimeMillis();
			bulkLoad.doBulkLoad(dir, (HTable)table);
			long bulkTime = System.currentTimeMillis()-start;
			System.out.println(table.getTableDescriptor().getNameAsString()+" bulkLoad time: "+bulkTime);
				
		}
		catch(java.io.IOException e){
			System.out.println("Bulk load taking longer than usual: sleeping for 10 minutes before proceeding to the next table..");
			Thread.sleep(600000);
			timedOut = true;
		}
		
		if (timedOut){//the BulkLoad took too long - which means the cluster is unbalanced
			try {
				con.getAdmin().split(table.getTableName());
				Thread.sleep(300000);//TODO find a better method to do these splits
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
		
	}
	
	public static void moveIdStringAssocDirectory(Path resourceIds, Path id2StringInput) throws IOException{
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(conf);
		Path source = new Path(resourceIds, TripleToResource.ID2STRING_DIR);
		FileUtil.copy(fs, source, fs, id2StringInput, true, false, conf);
	}
	
}
