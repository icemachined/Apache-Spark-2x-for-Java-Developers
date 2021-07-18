package com.packt.sfjd.ch11;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.graphx.Edge;
import org.apache.spark.graphx.EdgeTriplet;
import org.apache.spark.graphx.Graph;
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
import scala.Tuple2;
import scala.reflect.ClassTag;

import java.io.Serializable;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

import static org.apache.spark.sql.functions.*;

public class BuildDescendantsList {
	public static class Employee{
		public String name;
		public String role;

		public Employee(String name, String role) {
			this.name = name;
			this.role = role;
		}
	}
	public static class EmployeeMessage {
		Long currentId; // Tracks the most recent vertex appended to path and used for flagging isCyclic
		Integer level; // The number of up-line supervisors (level in reporting heirarchy)
		String head; // The top-most supervisor
		List<String> path; // The reporting path to the the top-most supervisor
		Boolean isCyclic; // Is the reporting structure of the employee cyclic
		Boolean isLeaf; // Is the employee rank and file (no down-line reporting employee)

		public EmployeeMessage(Long currentId, Integer level, String head, List<String> path, Boolean isCyclic, Boolean isLeaf) {
			this.currentId = currentId;
			this.level = level;
			this.head = head;
			this.path = path;
			this.isCyclic = isCyclic;
			this.isLeaf = isLeaf;
		}
	}
	// The structure of the vertex values of the graph
	public static class EmployeeValue {
		String name; // The employee name
		Long currentId; // Initial value is the employeeId
		Integer level; // Initial value is zero
		String head; // Initial value is this employee's name
		List<String> path; // Initial value contains this employee's name only
		Boolean isCyclic; // Initial value is false
		Boolean isLeaf;  // Initial value is true

		public EmployeeValue(String name, Long currentId, Integer level, String head, List<String> path, Boolean isCyclic, Boolean isLeaf) {
			this.name = name;
			this.currentId = currentId;
			this.level = level;
			this.head = head;
			this.path = path;
			this.isCyclic = isCyclic;
			this.isLeaf = isLeaf;
		}
	}
	public static class Emp2ValFn extends scala.runtime.AbstractFunction2<Object, Employee, EmployeeValue> implements Serializable {

		@Override
		public EmployeeValue apply(Object id, Employee v) {

			return new EmployeeValue(
					v.name,
					id,
					0,
					v.name,
					Arrays.asList(v.name),
					false,
					false
			);
		}

	}
	public static void main(String[] args) {
		//System.setProperty("hadoop.home.dir", "C:\\softwares\\Winutils");
		SparkConf conf = new SparkConf().setMaster("local[4]").setAppName("graph");
		SparkSession spark = SparkSession
				.builder().config(conf).getOrCreate();
		JavaSparkContext javaSparkContext = new JavaSparkContext(spark.sparkContext());
		ClassTag<String> stringTag = scala.reflect.ClassTag$.MODULE$.apply(String.class);
		ClassTag<Employee> empTag = scala.reflect.ClassTag$.MODULE$.apply(Employee.class);
		ClassTag<EmployeeValue> empvTag = scala.reflect.ClassTag$.MODULE$.apply(EmployeeValue.class);

		List<Row> dataTraining = Arrays.asList(
				RowFactory.create(1L, "Steve", "Jobs", "CEO", 1L),
				RowFactory.create(2L, "Leslie", "Lamport", "CTO", 1L),
				RowFactory.create(3L, "Jason", "Fried", "Manager", 1L),
				RowFactory.create(4L, "Joel", "Spolsky", "Manager", 2L),
				RowFactory.create(5L, "Jeff", "Dean", "Lead", 4L),
				RowFactory.create(6L, "Martin", "Odersky", "Sr.Dev", 5L),
				RowFactory.create(7L, "Linus", "Trovalds", "Dev", 6L),
				RowFactory.create(8L, "Steve", "Wozniak", "Dev", 6L),
				RowFactory.create(9L, "Matei", "Zaharia", "Dev", 6L),
				RowFactory.create(10L, "James", "Faeldon", "Intern", 7L)
		);
		StructType schema = new StructType(new StructField[]{
				new StructField("employeeId", DataTypes.LongType, false, Metadata.empty()),
				new StructField("firstName", DataTypes.StringType, false, Metadata.empty()),
				new StructField("lastName", DataTypes.StringType, false, Metadata.empty()),
				new StructField("role", DataTypes.StringType, false, Metadata.empty()),
				new StructField("supervisorId",DataTypes.LongType, false, Metadata.empty())
		});
		Dataset<Row> employeeDF = spark.createDataFrame(dataTraining, schema);
		JavaRDD<Tuple2<Object, Employee>> verticesRDD = employeeDF.select(col("employeeId"), concat(col("firstName"), lit(" "), col("lastName")), col("role")).javaRDD()
				.map(emp -> new Tuple2(emp.getLong(0), new Employee(emp.getString(1), emp.getString(2)));
		JavaRDD<Edge<String>> edgesRDD = employeeDF.select(col("supervisorId"), col("employeeId"), col("role")).javaRDD()
				.map(emp -> new Edge(emp.getLong(0), emp.getLong(1), emp.getString(2)));



// Define a default employee in case there are missing employee referenced in Graph
		Employee missingEmployee = new Employee("John Doe", "Unknown");
// Let's build the graph model


		Graph<Employee, String> employeeGraph = Graph.apply(verticesRDD.rdd(), edgesRDD.rdd(), missingEmployee,
				StorageLevel.MEMORY_ONLY(), StorageLevel.MEMORY_ONLY(), empTag, stringTag);


// add more dummy attributes to the vertices - id, level, root, path, iscyclic, existing value of current vertex to build path, isleaf, pk
		Graph<EmployeeValue, String> employeeValueGraph  = employeeGraph.mapVertices ( new Emp2ValFn(), empvTag, null); //
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

	/**
	 * Step 1: Mutate the value of the vertices, based on the message received
	 */
	public static void vprog(
			vertexId: VertexId,
			value: EmployeeValue,
			message: EmployeeMessage
	): EmployeeValue = {
		if (message.level == 0) { //superstep 0 - initialize
			value.copy(level = value.level + 1)
		} else if (message.isCyclic) { // set isCyclic
			value.copy(isCyclic = true)
		} else if (!message.isLeaf) { // set isleaf
			value.copy(isLeaf = false)
		} else { // set new values
			value.copy(
					currentId = message.currentId,
					level = value.level + 1,
					head = message.head,
					path = value.name :: message.path
    )
		}
	}
	/**
	 * Step 2: For all triplets that received a message -- meaning, any of the two vertices
	 * received a message from the previous step -- then compose and send a message.
	 */
	public static Iterator<Tuple2<Object, EmployeeMessage>> sendMsg(
			 EdgeTriplet<EmployeeValue, String> triplet
	) {
		EmployeeValue src = triplet.srcAttr();
		EmployeeValue dst = triplet.dstAttr();
		// Handle cyclic reporting structure
		if (src.currentId == triplet.dstId() || src.currentId == dst.currentId) {
			if (!src.isCyclic) { // Set isCyclic
				return new Iterator((triplet.dstId(), new EmployeeMessage(
						src.currentId,
						src.level,
						src.head,
						src.path,
						true,
						src.isLeaf
				)));
			} else { // Already marked as isCyclic (possibly, from previous superstep) so ignore
				Iterator.empty
			}
		} else { // Regular reporting structure
			if (src.isLeaf) { // Initially every vertex is leaf. Since this is a source then it should NOT be a leaf, update
				Iterator((triplet.srcId, EmployeeMessage(
						currentId = src.currentId,
						level = src.level,
						head = src.head,
						path = src.path,
						isCyclic = false,
						isLeaf = false // This is the only important value here
				)))
			} else { // Set new values by propagating source values to destination
				//Iterator.empty
				Iterator((triplet.dstId, EmployeeMessage(
						currentId = src.currentId,
						level = src.level,
						head = src.head,
						path = src.path,
						isCyclic = false, // Set to false so that cyclic updating is ignored in vprog
						isLeaf = true // Set to true so that leaf updating is ignored in vprog
				)))
			}
		}
	}
	/**
	 * Step 3: Merge all inbound messages to a vertex. No special merging needed for this use case.
	 */
	public static EmployeeMessage mergeMsg(EmployeeMessage msg1, EmployeeMessage msg2) { return msg2; }
}
