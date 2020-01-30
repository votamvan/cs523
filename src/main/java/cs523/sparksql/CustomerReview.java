package cs523.sparksql;

import java.io.IOException;
import java.io.Serializable;
import java.util.Arrays;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.io.compress.Compression.Algorithm;
import org.apache.hadoop.hbase.mapreduce.TableInputFormat;
import org.apache.hadoop.hbase.mapreduce.TableOutputFormat;
import org.apache.hadoop.hbase.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.protobuf.generated.ClientProtos;
import org.apache.hadoop.hbase.util.Base64;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.mapreduce.Job;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.SQLContext;

import scala.Tuple2;

public class CustomerReview implements Serializable {
	private static final long serialVersionUID = 1L;
	private String marketplace;
	private String customer_id;
	private String review_id;
	private String product_id;
	private String product_parent;
	private String product_title;
	private String product_category;
	private String star_rating;
	private String helpful_votes;
	private String total_votes;
	private String vine;
	private String verified_purchase;
	private String review_headline;
	private String review_body;
	private String review_date;
	private static final String DELIMITED = "\t";

	private CustomerReview(String marketplace, String customer_id, String review_id, String product_id,
			String product_parent, String product_title, String product_category, String star_rating,
			String helpful_votes, String total_votes, String vine, String verified_purchase, String review_headline,
			String review_body, String review_date) {
		super();
		this.marketplace = marketplace;
		this.customer_id = customer_id;
		this.review_id = review_id;
		this.product_id = product_id;
		this.product_parent = product_parent;
		this.product_title = product_title;
		this.product_category = product_category;
		this.star_rating = star_rating;
		this.helpful_votes = helpful_votes;
		this.total_votes = total_votes;
		this.vine = vine;
		this.verified_purchase = verified_purchase;
		this.review_headline = review_headline;
		this.review_body = review_body;
		this.review_date = review_date;
	}

	public static CustomerReview Parser(String reviewline) {
		List<String> review = Arrays.asList(reviewline.split(DELIMITED));
		return new CustomerReview(review.get(0), review.get(1), review.get(2), review.get(3), review.get(4),
				review.get(5), review.get(6), review.get(7), review.get(8), review.get(9), review.get(10),
				review.get(11), review.get(12), review.get(13), review.get(14));
	}

	public String getMarketplace() {
		return marketplace;
	}

	public String getCustomer_id() {
		return customer_id;
	}

	public String getReview_id() {
		return review_id;
	}

	public String getProduct_id() {
		return product_id;
	}

	public String getProduct_parent() {
		return product_parent;
	}

	public String getProduct_title() {
		return product_title;
	}

	public String getProduct_category() {
		return product_category;
	}

	public String getStar_rating() {
		return star_rating;
	}

	public String getHelpful_votes() {
		return helpful_votes;
	}

	public String getTotal_votes() {
		return total_votes;
	}

	public String getVine() {
		return vine;
	}

	public String getVerified_purchase() {
		return verified_purchase;
	}

	public String getReview_headline() {
		return review_headline;
	}

	public String getReview_body() {
		return review_body;
	}

	public String getReview_date() {
		return review_date;
	}

	public String toString() {
		return this.marketplace + DELIMITED + this.customer_id + DELIMITED + this.review_id + DELIMITED
				+ this.product_id + DELIMITED + this.product_parent + DELIMITED + this.product_title + DELIMITED
				+ this.product_category + DELIMITED + this.star_rating + DELIMITED + this.helpful_votes + DELIMITED
				+ this.total_votes + DELIMITED + this.vine + DELIMITED + this.verified_purchase + DELIMITED
				+ this.review_headline + DELIMITED + this.review_body + DELIMITED + this.review_date;
	}

	public static class HbaseTable {
		private static final String TABLE_NAME = "CustomerReview";
		private static final String CF_DEFAULT = "review_data";

		private static final String COL_MARKET_PLACE = "marketplace";
		private static final String COL_CUSTUMER_ID = "customer_id";
		private static final String COL_REVIEW_ID = "review_id";
		private static final String COL_PRODUCT_ID = "product_id";
		private static final String COL_PRODUCT_PARENT = "product_parent";
		private static final String COL_PRODUCT_TITLE = "product_title";
		private static final String COL_PRODUCT_CATEGORY = "product_category";
		private static final String COL_STAR_RATE = "star_rating";
		private static final String COL_HELPFUL_VOTES = "helpful_votes";
		private static final String COL_TOTAL_VOTES = "total_votes";
		private static final String COL_VINE = "vine";
		private static final String COL_VERIFIED_PURCHASE = "verified_purchase";
		private static final String COL_REVIEW_HEADLINE = "review_headline";
		private static final String COL_REVIEW_BODY = "review_body";
		private static final String COL_REVIEW_DATE = "review_date";
		
		private static SQLContext _sqlContext;

		@Deprecated
		public static void SaveToHbase(JavaRDD<CustomerReview> reviewRecords)
				throws MasterNotRunningException, Exception {
			
			//create connection with HBase
			Configuration config = null;

			config = HBaseConfiguration.create();
			HBaseAdmin.checkHBaseAvailable(config);
			System.out.println("HBase is running!");

			Connection connection = ConnectionFactory.createConnection(config);
			Admin admin = connection.getAdmin();

			HTableDescriptor table = new HTableDescriptor(TableName.valueOf(TABLE_NAME));
			table.addFamily(new HColumnDescriptor(CF_DEFAULT).setCompressionType(Algorithm.NONE));

			if (!admin.tableExists(table.getTableName())) {
				System.out.print("Creating table.... ");
				admin.createTable(table);
			}

			config.set(TableInputFormat.INPUT_TABLE, TABLE_NAME);

			// new Hadoop API configuration
			Job newAPIJobConfiguration = Job.getInstance(config);
			newAPIJobConfiguration.getConfiguration().set(TableOutputFormat.OUTPUT_TABLE, TABLE_NAME);
			newAPIJobConfiguration.setOutputFormatClass(TableOutputFormat.class);

			// create Key, Value pair to store in HBase
			JavaPairRDD<ImmutableBytesWritable, Put> hbasePuts = reviewRecords

					.mapToPair(new PairFunction<CustomerReview, ImmutableBytesWritable, Put>() {
						/**
						 * 
						 */
						private static final long serialVersionUID = 1L;

						@Override
						public Tuple2<ImmutableBytesWritable, Put> call(CustomerReview reviewRecord) throws Exception {

							Put put = new Put(Bytes.toBytes(reviewRecord.getReview_id()));
							put.add(Bytes.toBytes(CF_DEFAULT), Bytes.toBytes(COL_MARKET_PLACE),
									Bytes.toBytes(reviewRecord.getMarketplace()));
							put.add(Bytes.toBytes(CF_DEFAULT), Bytes.toBytes(COL_CUSTUMER_ID),
									Bytes.toBytes(reviewRecord.getCustomer_id()));

							put.add(Bytes.toBytes(CF_DEFAULT), Bytes.toBytes(COL_REVIEW_ID),
									Bytes.toBytes(reviewRecord.getReview_id()));
							put.add(Bytes.toBytes(CF_DEFAULT), Bytes.toBytes(COL_PRODUCT_ID),
									Bytes.toBytes(reviewRecord.getProduct_id()));

							put.add(Bytes.toBytes(CF_DEFAULT), Bytes.toBytes(COL_PRODUCT_PARENT),
									Bytes.toBytes(reviewRecord.getProduct_parent()));
							put.add(Bytes.toBytes(CF_DEFAULT), Bytes.toBytes(COL_PRODUCT_TITLE),
									Bytes.toBytes(reviewRecord.getProduct_title()));

							put.add(Bytes.toBytes(CF_DEFAULT), Bytes.toBytes(COL_PRODUCT_CATEGORY),
									Bytes.toBytes(reviewRecord.getProduct_category()));
							put.add(Bytes.toBytes(CF_DEFAULT), Bytes.toBytes(COL_STAR_RATE),
									Bytes.toBytes(reviewRecord.getStar_rating()));

							put.add(Bytes.toBytes(CF_DEFAULT), Bytes.toBytes(COL_HELPFUL_VOTES),
									Bytes.toBytes(reviewRecord.getHelpful_votes()));
							put.add(Bytes.toBytes(CF_DEFAULT), Bytes.toBytes(COL_TOTAL_VOTES),
									Bytes.toBytes(reviewRecord.getTotal_votes()));

							put.add(Bytes.toBytes(CF_DEFAULT), Bytes.toBytes(COL_VINE),
									Bytes.toBytes(reviewRecord.getVine()));
							put.add(Bytes.toBytes(CF_DEFAULT), Bytes.toBytes(COL_VERIFIED_PURCHASE),
									Bytes.toBytes(reviewRecord.getVerified_purchase()));

							put.add(Bytes.toBytes(CF_DEFAULT), Bytes.toBytes(COL_REVIEW_HEADLINE),
									Bytes.toBytes(reviewRecord.getReview_headline()));
							put.add(Bytes.toBytes(CF_DEFAULT), Bytes.toBytes(COL_REVIEW_BODY),
									Bytes.toBytes(reviewRecord.getReview_body()));

							put.add(Bytes.toBytes(CF_DEFAULT), Bytes.toBytes(COL_REVIEW_DATE),
									Bytes.toBytes(reviewRecord.getReview_date()));

							return new Tuple2<ImmutableBytesWritable, Put>(new ImmutableBytesWritable(), put);
						}
					});

			// save to HBase- Spark built-in API method
			hbasePuts.saveAsNewAPIHadoopDataset(newAPIJobConfiguration.getConfiguration());
		}

		public static JavaPairRDD<String, CustomerReview> ReadFromHbase(JavaSparkContext jsc) throws IOException {
			
			// SparkConf sparkConf = new SparkConf();

			// we can also run it at local:"local[3]" the number 3 means 3 threads
			// sparkConf.setMaster("local").setAppName(appName);
			// JavaSparkContext jsc = new JavaSparkContext(sparkConf);

			Configuration conf = HBaseConfiguration.create();
			
			Scan scan = new Scan();
			scan.addFamily(Bytes.toBytes(CF_DEFAULT));

			scan.addColumn(Bytes.toBytes(CF_DEFAULT), Bytes.toBytes(COL_MARKET_PLACE));
			scan.addColumn(Bytes.toBytes(CF_DEFAULT), Bytes.toBytes(COL_CUSTUMER_ID));
			scan.addColumn(Bytes.toBytes(CF_DEFAULT), Bytes.toBytes(COL_REVIEW_ID));

			scan.addColumn(Bytes.toBytes(CF_DEFAULT), Bytes.toBytes(COL_PRODUCT_ID));
			scan.addColumn(Bytes.toBytes(CF_DEFAULT), Bytes.toBytes(COL_PRODUCT_PARENT));
			scan.addColumn(Bytes.toBytes(CF_DEFAULT), Bytes.toBytes(COL_PRODUCT_TITLE));
			scan.addColumn(Bytes.toBytes(CF_DEFAULT), Bytes.toBytes(COL_PRODUCT_CATEGORY));
			scan.addColumn(Bytes.toBytes(CF_DEFAULT), Bytes.toBytes(COL_STAR_RATE));
			scan.addColumn(Bytes.toBytes(CF_DEFAULT), Bytes.toBytes(COL_HELPFUL_VOTES));
			scan.addColumn(Bytes.toBytes(CF_DEFAULT), Bytes.toBytes(COL_TOTAL_VOTES));
			scan.addColumn(Bytes.toBytes(CF_DEFAULT), Bytes.toBytes(COL_VINE));
			scan.addColumn(Bytes.toBytes(CF_DEFAULT), Bytes.toBytes(COL_VERIFIED_PURCHASE));
			scan.addColumn(Bytes.toBytes(CF_DEFAULT), Bytes.toBytes(COL_REVIEW_HEADLINE));
			scan.addColumn(Bytes.toBytes(CF_DEFAULT), Bytes.toBytes(COL_REVIEW_BODY));
			scan.addColumn(Bytes.toBytes(CF_DEFAULT), Bytes.toBytes(COL_REVIEW_DATE));

			String scanToString = "";

			ClientProtos.Scan proto = ProtobufUtil.toScan(scan);
			scanToString = Base64.encodeBytes(proto.toByteArray());

			conf.set(TableInputFormat.INPUT_TABLE, TABLE_NAME);
			conf.set(TableInputFormat.SCAN, scanToString);

			// get the Result of query from the Table of Hbase
			JavaPairRDD<ImmutableBytesWritable, Result> hBaseRDD = jsc.newAPIHadoopRDD(conf, TableInputFormat.class,
					ImmutableBytesWritable.class, Result.class);

			JavaPairRDD<String, CustomerReview> review_records = hBaseRDD
					.mapToPair(new PairFunction<Tuple2<ImmutableBytesWritable, Result>, String, CustomerReview>() {
						/**
						 * 
						 */
						private static final long serialVersionUID = 1L;

						@Override
						public Tuple2<String, CustomerReview> call(Tuple2<ImmutableBytesWritable, Result> results) {

							byte[] marketplace = results._2().getValue(Bytes.toBytes(CF_DEFAULT),
									Bytes.toBytes(COL_MARKET_PLACE));
							byte[] customer_id = results._2().getValue(Bytes.toBytes(CF_DEFAULT),
									Bytes.toBytes(COL_CUSTUMER_ID));
							byte[] review_id = results._2().getValue(Bytes.toBytes(CF_DEFAULT),
									Bytes.toBytes(COL_REVIEW_ID));

							byte[] product_id = results._2().getValue(Bytes.toBytes(CF_DEFAULT),
									Bytes.toBytes(COL_PRODUCT_ID));
							byte[] product_parent = results._2().getValue(Bytes.toBytes(CF_DEFAULT),
									Bytes.toBytes(COL_PRODUCT_PARENT));
							byte[] product_title = results._2().getValue(Bytes.toBytes(CF_DEFAULT),
									Bytes.toBytes(COL_PRODUCT_TITLE));
							byte[] product_category = results._2().getValue(Bytes.toBytes(CF_DEFAULT),
									Bytes.toBytes(COL_PRODUCT_CATEGORY));
							byte[] star_rating = results._2().getValue(Bytes.toBytes(CF_DEFAULT),
									Bytes.toBytes(COL_STAR_RATE));
							byte[] helpful_votes = results._2().getValue(Bytes.toBytes(CF_DEFAULT),
									Bytes.toBytes(COL_HELPFUL_VOTES));
							byte[] total_votes = results._2().getValue(Bytes.toBytes(CF_DEFAULT),
									Bytes.toBytes(COL_TOTAL_VOTES));
							byte[] vine = results._2().getValue(Bytes.toBytes(CF_DEFAULT), Bytes.toBytes(COL_VINE));
							byte[] verified_purchase = results._2().getValue(Bytes.toBytes(CF_DEFAULT),
									Bytes.toBytes(COL_VERIFIED_PURCHASE));
							byte[] review_headline = results._2().getValue(Bytes.toBytes(CF_DEFAULT),
									Bytes.toBytes(COL_REVIEW_HEADLINE));
							byte[] review_body = results._2().getValue(Bytes.toBytes(CF_DEFAULT),
									Bytes.toBytes(COL_REVIEW_BODY));
							byte[] review_date = results._2().getValue(Bytes.toBytes(CF_DEFAULT),
									Bytes.toBytes(COL_REVIEW_DATE));

							//if(!Bytes.toString(marketplace).equals(COL_MARKET_PLACE))
							//{
							CustomerReview review = new CustomerReview(Bytes.toString(marketplace),
									Bytes.toString(customer_id), Bytes.toString(review_id), Bytes.toString(product_id),
									Bytes.toString(product_parent), Bytes.toString(product_title),
									Bytes.toString(product_category), Bytes.toString(star_rating),
									Bytes.toString(helpful_votes), Bytes.toString(total_votes), Bytes.toString(vine),
									Bytes.toString(verified_purchase), Bytes.toString(review_headline),
									Bytes.toString(review_body), Bytes.toString(review_date));

							return new Tuple2<String, CustomerReview>(Bytes.toString(results._1().get()), review);
							//}
						}
					});
			
			_sqlContext = new SQLContext(jsc);
			_sqlContext.createDataFrame(review_records.values(), CustomerReview.class).registerTempTable(TABLE_NAME);
		    
		    //df.registerTempTable(TABLE_NAME);
			//_sqlContext.setLogLevel("OFF");
		    return review_records;
		}
		
		public static DataFrame get_verified_purchase()
		{
			return exce_sql(sql_verified_purchase());
		}
		public static DataFrame get_ratings_marketplace()
		{
			return exce_sql(sql_ratings_marketplace());
		}
		public static DataFrame get_vine()
		{
			return exce_sql(sql_vine());
		}
		public static DataFrame get_product_category_rateing()
		{
			return exce_sql(sql_product_category_rateing());
		}
		public static DataFrame get_helpful_rateing()
		{
			return exce_sql(sql_helpful_rateing());
		}
		public static DataFrame get_customer_rateing()
		{
			return exce_sql(sql_customer_rating());
		}
		public static DataFrame get_customer_rateing_verified()
		{
			return exce_sql(sql_customer_rating_verified());
		}
		public static DataFrame get_helpful_votes()
		{
			return exce_sql(sql_helpful_votes());
		}
		private static DataFrame exce_sql(String sql)
		{  
		    //DataFrame df = _sqlContext.createDataFrame(records.values(), CustomerReview.class);
		    
		    //df.registerTempTable(TABLE_NAME);
		    
			return _sqlContext.sql(sql);
		}
		private static String sql_helpful_votes() 
		{
			return "select * from " + TABLE_NAME +" " + 
					"order by helpful_votes " + 
					"desc limit 15";
		}
		private static String sql_customer_rating_verified() 
		{
			return "select customer_id, " + 
					"count(*) AS COUNT, " + 
					"count(distinct product_category) cats, " + 
					"cast(avg(cast(star_rating as decimal(5,4))) as decimal (3,2)) avg_rating, " + 
					"cast(avg(cast (helpful_votes as decimal (18,4))) as decimal (16,2)) avg_help, " + 
					"sum(case when star_rating = 1 then 1 else 0 end) one, " + 
					"sum(case when star_rating = 2 then 1 else 0 end) two, " + 
					"sum(case when star_rating = 3 then 1 else 0 end) three, " + 
					"sum(case when star_rating = 4 then 1 else 0 end) four, " + 
					"sum(case when star_rating = 5 then 1 else 0 end) five, " + 
					"sum(case when vine = 'Y' then 1 else 0 end) vine_reviews, " + 
					"sum(case when verified_purchase = 'Y' then 1 else 0 end) verified_purchases " + 
					"from "+ TABLE_NAME +" " + 
					"group by customer_id, 1 " + 
					"order by 2 desc " + 
					"limit 10";
		}
		private static String sql_customer_rating() 
		{
			return "select customer_id, " + 
					"count(*) as COUNT, " + 
					"count(distinct product_category) cats, " + 
					"cast(avg(cast(star_rating as decimal(5,4))) as decimal (3,2)) avg_rating, " + 
					"sum(case when star_rating = 1 then 1 else 0 end) one, " + 
					"sum(case when star_rating = 2 then 1 else 0 end) two, " + 
					"sum(case when star_rating = 3 then 1 else 0 end) three, " + 
					"sum(case when star_rating = 4 then 1 else 0 end) four, " + 
					"sum(case when star_rating = 5 then 1 else 0 end) five " + 
					"from "+ TABLE_NAME +" " + 
					"group by customer_id, 1 " + 
					"order by 2 desc " + 
					"limit 10";
		}
		private static String sql_helpful_rateing() 
		{
			return "select star_rating, " + 
					"cast(avg(cast (helpful_votes as decimal (18,4))) as decimal (16,2)) AVG_HELP, " + 
					"count(*) AS COUNT " + 
					"from "+ TABLE_NAME +" " + 
					"group by star_rating, 1 " + 
					"order by 1";
		}
		private static String sql_product_category_rateing() 
		{
			return "select product_category, " + 
					"cast(avg(cast(star_rating as decimal(5,4))) as decimal (3,2)) avg_rating, " + 
					"count(*) AS COUNT " + 
					"from "+ TABLE_NAME +" " + 
					"group by product_category, 1 " + 
					"order by 2";
		}
		private static String sql_vine()
		{
			return "select vine, "+
			"cast(avg(cast(star_rating as decimal(5,4))) as decimal (3,2)) avg_rating, "+
			"count(*) as COUNT "+
			" from "+ TABLE_NAME +
			" group by vine, 1"+ 
			" order by 1";
		}
		private static String sql_ratings_marketplace()
		{
			return "select marketplace, "+
					"cast(avg(cast(star_rating as decimal(5,4))) as decimal (3,2)) avg_rating, "+
					"count(*) as COUNT "+
					"from "+TABLE_NAME+" group by marketplace, 1 order by 1";
		}
		private static String sql_verified_purchase()
		{
			return "select verified_purchase, "+
					"cast(avg(cast(star_rating as decimal(5,4))) as decimal (3,2)) avg_rating,"+
					"count(*) as COUNT "+
					"from "+TABLE_NAME+" group by verified_purchase, 1 order by 1 ";
		}
	}
}
