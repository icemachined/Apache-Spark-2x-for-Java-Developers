package com.packt.sfjd.ch11;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.graphx.Edge;
import org.apache.spark.graphx.Graph;
import org.apache.spark.ml.linalg.VectorUDT;
import org.apache.spark.ml.linalg.Vectors;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.storage.StorageLevel;
import scala.Predef;
import scala.reflect.ClassTag;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class BuildDescendantsListFromEdges {
	public static void main(String[] args) {
		//System.setProperty("hadoop.home.dir", "C:\\softwares\\Winutils");
		SparkConf conf = new SparkConf().setMaster("local[4]").setAppName("graph");
		SparkSession spark = SparkSession
				.builder().config(conf).getOrCreate();
		JavaSparkContext javaSparkContext = new JavaSparkContext(spark.sparkContext());
		ClassTag<String> stringTag = scala.reflect.ClassTag$.MODULE$.apply(String.class);

		List<Row> dataTraining = Arrays.asList(
				RowFactory.create(1L, 1L, "LEGAL"),
				RowFactory.create(2L, 1L, "LEGAL"),
				RowFactory.create(3L, 2L, "LEGAL"),
				RowFactory.create(4L, 2L, "LEGAL"),
				RowFactory.create(5L, 4L, "LEGAL")
		);
		StructType schema = new StructType(new StructField[]{
				new StructField("id", DataTypes.LongType, false, Metadata.empty()),
				new StructField("parent_id",DataTypes.LongType, false, Metadata.empty()),
				new StructField("type",DataTypes.StringType, false, Metadata.empty())
		});
		Dataset<Row> hierachy = spark.createDataFrame(dataTraining, schema);


// create the edge rdd
// top down relationship
		JavaRDD<Edge<String>>  edgeRDD = hierachy.javaRDD().map(x -> new Edge<String>((Long)x.get(0),(Long)x.get(1), (String)x.get(2)));

//		List<Edge<String>> edges = new ArrayList<>();
//
//		edges.add(new Edge<String>(1, 2, "Friend"));
//		edges.add(new Edge<String>(2, 3, "Advisor"));
//		edges.add(new Edge<String>(1, 3, "Friend"));
//		edges.add(new Edge<String>(4, 3, "colleague"));
//		edges.add(new Edge<String>(4, 5, "Relative"));
//		edges.add(new Edge<String>(2, 5, "BusinessPartners"));
//
//
//		JavaRDD<Edge<String>> edgeRDD = javaSparkContext.parallelize(edges);
		
		
		Graph<String, String> graph = Graph.fromEdges(edgeRDD.rdd(), "",StorageLevel.MEMORY_ONLY(), StorageLevel.MEMORY_ONLY(), stringTag, stringTag);
		
		
		graph.vertices().toJavaRDD().collect().forEach(System.out::println);

		Row initialmsg = RowFactory.create((0l,0,0,1);

// add more dummy attributes to the vertices - id, level, root, path, iscyclic, existing value of current vertex to build path, isleaf, pk
		Graph<String, String> initialgraph = graph.mapVertices((id, v) -> RowFactory.create(id, 0, id, 0, v, 1, id), stringTag, Predef.$eq$colon$eq$.MODULE$.tpEquals());
//
//		val hrchyrdd = initialgraph.pregel(initialmsg,
//				int.maxvalue,
//				edgedirection.out)(
//				setmsg,
//				sendmsg,
//				mergemsg)
//
//
//// build the path from the list
//		val hrchyoutrdd = hrchyrdd.vertices.map{case(id,v) => (v._8,(v._2,v._3,pathseperator + v._4.reverse.mkstring(pathseperator),v._5, v._7 )) }


//	graph.aggregateMessages(sendMsg, mergeMsg, tripletFields, evidence$11)	
		
	}
	//mutate the value of the vertices
//	public static void setmsg(vertexid: vertexid, value: (long,int,any,list[string], int,string,int,any), message: (long,int, any,list[string],int,int)): (long,int, any,list[string],int,string,int,any) = {
//		if (message._2 < 1) { //superstep 0 - initialize
//			(value._1,value._2+1,value._3,value._4,value._5,value._6,value._7,value._8)
//		} else if ( message._5 == 1) { // set iscyclic
//			(value._1, value._2, value._3, value._4, message._5, value._6, value._7,value._8)
//		} else if ( message._6 == 0 ) { // set isleaf
//			(value._1, value._2, value._3, value._4, value._5, value._6, message._6,value._8)
//		} else { // set new values
//			( message._1,value._2+1, message._3, value._6 :: message._4 , value._5,value._6,value._7,value._8)
//		}
//	}
//
//
//
//	// send the value to vertices
//	def sendmsg(triplet: edgetriplet[(long,int,any,list[string],int,string,int,any), _]): iterator[(vertexid, (long,int,any,list[string],int,int))] = {
//		val sourcevertex = triplet.srcattr
//		val destinationvertex = triplet.dstattr
//// check for icyclic
//		if (sourcevertex._1 == triplet.dstid || sourcevertex._1 == destinationvertex._1)
//			if (destinationvertex._5==0) { //set iscyclic
//				iterator((triplet.dstid, (sourcevertex._1, sourcevertex._2, sourcevertex._3,sourcevertex._4, 1,sourcevertex._7)))
//			} else {
//				iterator.empty
//			}
//		else {
//			if (sourcevertex._7==1) //is not leaf
//			{
//				iterator((triplet.srcid, (sourcevertex._1,sourcevertex._2,sourcevertex._3, sourcevertex._4 ,0, 0 )))
//			}
//			else { // set new values
//				iterator((triplet.dstid, (sourcevertex._1, sourcevertex._2, sourcevertex._3, sourcevertex._4, 0, 1)))
//			}
//		}
//	}
//
//
//
//	// receive the values from all connected vertices
//	def mergemsg(msg1: (long,int,any,list[string],int,int), msg2: (long,int, any,list[string],int,int)): (long,int,any,list[string],int,int) = {
//// dummy logic not applicable to the data in this usecase
//		msg2
//	}

}
