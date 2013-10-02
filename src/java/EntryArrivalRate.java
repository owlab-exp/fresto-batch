import java.util.List;
import java.util.ArrayList;
import java.util.Map;
import java.util.HashMap;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import cascalog.CascalogFunction;
import cascalog.ops.RandLong;
import cascalog.ops.IdentityBuffer;

import jcascalog.Api;
import jcascalog.Subquery;
import jcascalog.op.Count;
import jcascalog.op.Sum;

import com.backtype.hadoop.pail.Pail;
import com.backtype.hadoop.pail.PailSpec;
import com.backtype.hadoop.pail.PailStructure;

import com.backtype.cascading.tap.PailTap;
import com.backtype.cascading.tap.PailTap.PailTapOptions;

import cascading.tap.Tap;
import cascading.tuple.Tuple;
import cascading.flow.FlowProcess;
import cascading.operation.FunctionCall;

import com.twitter.maple.tap.StdoutTap;

import fresto.data.FrestoData;
import fresto.data.DataUnit;
import fresto.data.EntryOperationCallEdge;

//import fresto.pail.SplitFrestoDataPailStructure;

public class EntryArrivalRate {
    	private static final String NEW_PATH = "hdfs://fresto1.owlab.com:9000/fresto/new";
    	private static final String MASTER_PATH = "hdfs://fresto1.owlab.com:9000/fresto/master";
    	private static final String SNAPSHOT_PATH = "hdfs://fresto1.owlab.com:9000/fresto/tmp/swa/snapshot";
    	private static final String SHREDDED_PATH = "hdfs://fresto1.owlab.com:9000/fresto/tmp/swa/shredded";
    
	public static void main(String[] args) throws Exception {
	    //while(true) {
	    //    makeSnapshot();
	    //    appendToMaster();
	    //}
	    test01();
	}
	
	public static void test01() {

		// Start common block of every job
		System.setProperty("HADOOP_USER_NAME", "hdfs");
	
		Map conf = new HashMap();
		String sers = "backtype.hadoop.ThriftSerialization";
		sers += ",";
		sers += "org.apache.hadoop.io.serializer.WritableSerialization";
		conf.put("io.serializations", sers);
		Api.setApplicationConf(conf);
	
	
		Configuration hadoopConfig = new Configuration();
		hadoopConfig.set("fs.default.name", "hdfs://fresto1.owlab.com:9000");
		// End common block of every job

		Tap sink = new StdoutTap();
		Tap source = splitFrestoDataTap(MASTER_PATH);
		// Sum by seconds
		Subquery rollupBySecond = new Subquery("?second-bucket", "?count")
					.predicate(source, "_", "?data")
					.predicate(new ExtractEntryCallTimestampFields(), "?data").out("?timestamp")
					//.predicate(new ToMinuteBucket(), "?timestamp").out("?minute-bucket")
					.predicate(new ToSecondBucket(), "?timestamp").out("?second-bucket")
					.predicate(new Count(), "?count");

		Api.execute(sink, rollupBySecond);

		// Sums by minute, ...
		Subquery arrivals = new Subquery("?granularity", "?bucket", "?total-count")
					.predicate(rollupBySecond, "?second-bucket", "?count")
					.predicate(new EmitGranularities(), "?second-bucket").out("?granularity", "?bucket")
					.predicate(new Sum(), "?count").out("?total-count");

		// Dplicate?
		//Api.execute(sink, arrivals);
	}

	public static class ExtractEntryCallTimestampFields extends CascalogFunction {
	    public void operate(FlowProcess process, FunctionCall call) {
		FrestoData data = (FrestoData) call.getArguments().getObject(0);
		DataUnit dataUnit = data.getDataUnit();
		if(dataUnit.getSetField() == DataUnit._Fields.ENTRY_OPERATION_CALL_EDGE) {
			call.getOutputCollector().add(new Tuple(dataUnit.getEntryOperationCallEdge().getTimestamp()));
		}
	    }
	}
	
	public static class EmitGranularities extends CascalogFunction {
	    public void operate(FlowProcess process, FunctionCall call) {
		long secondBucket = call.getArguments().getLong(0);

		long minuteBucket = secondBucket / 60;
		long tenMinuteBucket = minuteBucket / 10;
		long hourBucket = tenMinuteBucket / 6;
		long dayBucket = hourBucket / 24;
		long weekBucket = dayBucket / 7;
		long monthBucket = dayBucket / 28;

		call.getOutputCollector().add(new Tuple("m", "minute-bucket"));
		call.getOutputCollector().add(new Tuple("10m", "10minute-bucket"));
		call.getOutputCollector().add(new Tuple("h", "hour-bucket"));
		call.getOutputCollector().add(new Tuple("d", "day-bucket"));
		call.getOutputCollector().add(new Tuple("w", "week-bucket"));
		call.getOutputCollector().add(new Tuple("M", "month-bucket"));

	    }
	}

	public static class ToSecondBucket extends CascalogFunction {
	    private static final int SECOND_MILLIS = 1000; // 60 seconds, 1000 milliseconds
	    
	    public void operate(FlowProcess process, FunctionCall call) {
		long timestamp = call.getArguments().getLong(0);
		long minuteBucket = timestamp/SECOND_MILLIS;
		call.getOutputCollector().add(new Tuple(minuteBucket));
	    }
	}

	public static class ToMinuteBucket extends CascalogFunction {
	    private static final int MINUTE_MILLIS = 60 * 1000; // 60 seconds, 1000 milliseconds
	    
	    public void operate(FlowProcess process, FunctionCall call) {
		long timestamp = call.getArguments().getLong(0);
		long minuteBucket = timestamp/MINUTE_MILLIS;
		call.getOutputCollector().add(new Tuple(minuteBucket));
	    }
	}

	// Copied methods below	
	public static void makeSnapshot() throws IOException {
	
		Configuration hadoopConfig = new Configuration();
		hadoopConfig.set("fs.default.name", "hdfs://fresto1.owlab.com:9000");
		
		// Initialize swap directory
		FileSystem fs = FileSystem.get(hadoopConfig);
		fs.delete(new Path("/fresto/tmp/swa"), true);
		fs.mkdirs(new Path("/fresto/tmp/swa"));
	
		// Grap snapshot in the "new" pail directory.
		Pail<FrestoData> newDataPail = new Pail<FrestoData>(NEW_PATH);
		// Move the snapshot to a temporary location 
		Pail<FrestoData> snapshotPail = newDataPail.snapshot(SNAPSHOT_PATH);
		// Now ths snapshot pails moved to the temporary location, then delete the snapshot in the "new".
		newDataPail.deleteSnapshot(snapshotPail);
	
	}
	
	public static void appendToMaster() throws IOException {
		// Start common block of every job
		System.setProperty("HADOOP_USER_NAME", "hdfs");
	
		Map conf = new HashMap();
		String sers = "backtype.hadoop.ThriftSerialization";
		sers += ",";
		sers += "org.apache.hadoop.io.serializer.WritableSerialization";
		conf.put("io.serializations", sers);
		Api.setApplicationConf(conf);
	
	
		Configuration hadoopConfig = new Configuration();
		hadoopConfig.set("fs.default.name", "hdfs://fresto1.owlab.com:9000");
		// End common block of every job
		
		Pail<FrestoData> pail = new Pail<FrestoData>(SNAPSHOT_PATH);
		PailTapOptions opts = new PailTapOptions();
		opts.spec = pail.getSpec();
	
		Tap source = new PailTap(pail.getRoot(), opts);
		Tap sink = splitFrestoDataTap(SHREDDED_PATH);
	
		Subquery reduced = new Subquery("?rand", "?data")
					.predicate(source, "_", "?data-in")
					.predicate(new RandLong(), "?rand")
					.predicate(new IdentityBuffer(), "?data-in").out("?data");
		Api.execute(sink, new Subquery("?data")
					.predicate(reduced, "_", "?data"));
	
		Pail shreddedPail = new Pail(SHREDDED_PATH);
		shreddedPail.consolidate();
	
		//Pail<FrestoData> masterPail = Pail.create("hdfs://fresto1.owlab.com:9000/fresto/master", new SplitFrestoDataPailStructure());
		Pail<FrestoData> masterPail = new Pail<FrestoData>(MASTER_PATH);
		masterPail.absorb(shreddedPail);
		masterPail.consolidate();
	
	}
	
	// To specify structure when sinking
	public static PailTap splitFrestoDataTap(String path) {
	    PailTapOptions opts = new PailTapOptions();
	    //opts.spec = new PailSpec((PailStructure) new SplitFrestoDataPailStructure());
	    opts.spec = new PailSpec(new SplitFrestoDataPailStructure());
	    return new PailTap(path, opts);
	}
	
	// To specify structure when reading
	public static PailTap frestoAttributeTap(String path, final DataUnit._Fields... fields) {
	    PailTapOptions opts = new PailTapOptions();
	    opts.attrs = new List[] {
		new ArrayList<String>() {{
		    for(DataUnit._Fields field: fields) {
			add("" + field.getThriftFieldId());
		    }
		}}
		};
	    return new PailTap(path, opts);
	}



}
