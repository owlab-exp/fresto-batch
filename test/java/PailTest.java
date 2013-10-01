import java.util.List;
import java.util.ArrayList;
import java.util.Map;
import java.util.HashMap;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import cascalog.ops.RandLong;
import cascalog.ops.IdentityBuffer;
import jcascalog.Api;
import jcascalog.Subquery;
import com.backtype.hadoop.pail.Pail;
import com.backtype.hadoop.pail.PailSpec;
import com.backtype.hadoop.pail.PailStructure;
import cascading.tap.Tap;
import com.backtype.cascading.tap.PailTap;
import com.backtype.cascading.tap.PailTap.PailTapOptions;
import com.twitter.maple.tap.StdoutTap;

import fresto.data.FrestoData;
import fresto.data.DataUnit;

//import fresto.pail.SplitFrestoDataPailStructure;

public class PailTest {
	public static void main(String[] args) throws Exception {
		//moveTest();
		//queryTest02();
		//batchTest01();
		//batchTest02();
		//batchTest03();
		batchTest04();
	}
	
	public static void moveTest() throws IOException {
		// Common block of every job
		System.setProperty("HADOOP_USER_NAME", "hdfs");

		Map conf = new HashMap();
		String sers = "backtype.hadoop.ThriftSerialization";
		sers += ",";
		sers += "org.apache.hadoop.io.serializer.WritableSerialization";
		conf.put("io.serializations", sers);
		Api.setApplicationConf(conf);


		Configuration hadoopConfig = new Configuration();
		hadoopConfig.set("fs.default.name", "hdfs://fresto1.owlab.com:9000");
		
		// Initialize swap directory
		FileSystem fs = FileSystem.get(hadoopConfig);
		fs.delete(new Path("/fresto/tmp/swa"), true);
		fs.mkdirs(new Path("/fresto/tmp/swa"));

		// Grap snapshot in the "new" pail directory.
		Pail<FrestoData> newDataPail = new Pail<FrestoData>("hdfs://fresto1.owlab.com:9000/fresto/new");
		// Move the snapshot to a temporary location 
		Pail<FrestoData> snapshotPail = newDataPail.snapshot("hdfs://fresto1.owlab.com:9000/fresto/tmp/swa/snapshot");
		// Now ths snapshot pails moved to the temporary location, then delete the snapshot in the "new".
		newDataPail.deleteSnapshot(snapshotPail);
		
		// TODO to something 



	}

	public static void queryTest01() {
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
		
		Tap source = new PailTap("hdfs://fresto1.owlab.com:9000/fresto/tmp/swa/snapshot");

		Api.execute(new StdoutTap(),
			new Subquery("?data")
				.predicate(source, "_", "?data")
				);
	}

	public static void queryTest02() {
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
		

		PailTapOptions opts = new PailTapOptions();
		// To only read responseEdge records
		opts.attrs = new List[] {
		    		new ArrayList<String>() {{
				    add("" + DataUnit._Fields
				    		     //.REQUEST_EDGE
				    		     .RESPONSE_EDGE
						     .getThriftFieldId());
				}}
				};

		Tap requestEdges = new PailTap("hdfs://fresto1.owlab.com:9000/fresto/tmp/swa/snapshot", opts);

		Api.execute(new StdoutTap(),
			new Subquery("?data")
				.predicate(requestEdges, "_", "?data")
				);
	}
	
	public static void batchTest01() throws IOException {
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
		
		Pail<FrestoData> pail = new Pail<FrestoData>("hdfs://fresto1.owlab.com:9000/fresto/tmp/swa/snapshot");
		PailTapOptions opts = new PailTapOptions();
		opts.spec = pail.getSpec();

		Tap source = new PailTap(pail.getRoot(), opts);
		// Following is not working, even though this is from book!
		//Tap source = new PailTap("hdfs://fresto1.owlab.com:9000/fresto/tmp/swa/snapshot", opts);
		// Following is not working of course
		//Tap source = attributeTap("hdfs://fresto1.owlab.com:9000/fresto/tmp/swa/snapshot", DataUnit._Fields.ENTRY_OPERATION_CALL_EDGE);
		Tap sink = splitFrestoDataTap("hdfs://fresto1.owlab.com:9000/fresto/tmp/swa/shredded");

		Api.execute(sink,
			new Subquery("?data")
				.predicate(source, "_", "?data")
				);
	}
	
	public static void batchTest02() throws IOException {
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
		
		Pail<FrestoData> pail = new Pail<FrestoData>("hdfs://fresto1.owlab.com:9000/fresto/tmp/swa/snapshot");
		PailTapOptions opts = new PailTapOptions();
		opts.spec = pail.getSpec();

		Tap source = new PailTap(pail.getRoot(), opts);
		Tap sink = splitFrestoDataTap("hdfs://fresto1.owlab.com:9000/fresto/tmp/swa/shredded01");

		Subquery reduced = new Subquery("?rand", "?data")
					.predicate(source, "_", "?data-in")
					.predicate(new RandLong(), "?rand")
					.predicate(new IdentityBuffer(), "?data-in").out("?data");
		Api.execute(sink, new Subquery("?data")
					.predicate(reduced, "_", "?data"));

	}
	
	public static void batchTest03() throws IOException {
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
		
		Pail<FrestoData> pail = new Pail<FrestoData>("hdfs://fresto1.owlab.com:9000/fresto/tmp/swa/snapshot");
		PailTapOptions opts = new PailTapOptions();
		opts.spec = pail.getSpec();

		Tap source = new PailTap(pail.getRoot(), opts);
		Tap sink = splitFrestoDataTap("hdfs://fresto1.owlab.com:9000/fresto/tmp/swa/shredded02");

		Subquery reduced = new Subquery("?rand", "?data")
					.predicate(source, "_", "?data-in")
					.predicate(new RandLong(), "?rand")
					.predicate(new IdentityBuffer(), "?data-in").out("?data");
		Api.execute(sink, new Subquery("?data")
					.predicate(reduced, "_", "?data"));

		Pail shreddedPail = new Pail("hdfs://fresto1.owlab.com:9000/fresto/tmp/swa/shredded02");
		shreddedPail.consolidate();

	}
	
	public static void batchTest04() throws IOException {
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
		
		Pail<FrestoData> pail = new Pail<FrestoData>("hdfs://fresto1.owlab.com:9000/fresto/tmp/swa/snapshot");
		PailTapOptions opts = new PailTapOptions();
		opts.spec = pail.getSpec();

		Tap source = new PailTap(pail.getRoot(), opts);
		Tap sink = splitFrestoDataTap("hdfs://fresto1.owlab.com:9000/fresto/tmp/swa/shredded03");

		Subquery reduced = new Subquery("?rand", "?data")
					.predicate(source, "_", "?data-in")
					.predicate(new RandLong(), "?rand")
					.predicate(new IdentityBuffer(), "?data-in").out("?data");
		Api.execute(sink, new Subquery("?data")
					.predicate(reduced, "_", "?data"));

		Pail shreddedPail = new Pail("hdfs://fresto1.owlab.com:9000/fresto/tmp/swa/shredded02");
		shreddedPail.consolidate();

		Pail<FrestoData> masterPail = Pail.create("hdfs://fresto1.owlab.com:9000/fresto/master", new SplitFrestoDataPailStructure());
		masterPail.absorb(shreddedPail);

	}
	
	// To specify structure when sinking
	public static PailTap splitFrestoDataTap(String path) {
	    PailTapOptions opts = new PailTapOptions();
	    //opts.spec = new PailSpec((PailStructure) new SplitFrestoDataPailStructure());
	    opts.spec = new PailSpec(new SplitFrestoDataPailStructure());
	    return new PailTap(path, opts);
	}

	// To specify structure when reading
	public static PailTap attributeTap(String path, final DataUnit._Fields... fields) {
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
