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

public class InjestStep {
    	private static final String NEW_PATH = "hdfs://fresto1.owlab.com:9000/fresto/new";
    	private static final String MASTER_PATH = "hdfs://fresto1.owlab.com:9000/fresto/master";
    	private static final String SNAPSHOT_PATH = "hdfs://fresto1.owlab.com:9000/fresto/tmp/swa/snapshot";
    	private static final String SHREDDED_PATH = "hdfs://fresto1.owlab.com:9000/fresto/tmp/swa/shredded";
    
	public static void main(String[] args) throws Exception {
	    while(true) {
		makeSnapshot();
		appendToMaster();
	    }
	}
	
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
