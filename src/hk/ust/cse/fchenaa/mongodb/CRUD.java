package hk.ust.cse.fchenaa.mongodb;

import com.mongodb.MapReduceCommand;
import com.mongodb.MongoClient;
import com.mongodb.MongoException;
import com.mongodb.WriteConcern;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;
import com.mongodb.DBCursor;
import com.mongodb.MongoClientURI;
import com.mongodb.ServerAddress;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MapReduceIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;

import java.util.Arrays;
import java.util.Date;

import org.bson.Document;
import org.bson.conversions.Bson;
import org.bson.types.ObjectId;

public class CRUD {

	public static void main( String args[] ) {

		try{

			// To connect to mongodb server
			MongoClient mongoClient = new MongoClient( "localhost" , 27017 );

			// Now connect to your databases
			MongoDatabase db = mongoClient.getDatabase("test");
			//DB db = mongoClient.getDB( "test" );
			//System.out.println("Connect to database successfully");
			//boolean auth = db.authenticate(myUserName, myPassword);
			//System.out.println("Authentication: "+auth);


			//creating a collection
			MongoCollection<Document> coll = db.getCollection("mycol");
			//coll.drop();


			//inserting a document
			Document doc = new Document("title", "MongoDB").
					append("description", "database").
					append("likes", 100).
					append("url", "http://www.tutorialspoint.com/mongodb/").
					append("by", "tutorials point");

			coll.insertOne(doc);        

			Document item1Doc = new Document("sku", "mmm").
					append("qty", 5).
					append("price", 2.5);
			Document item2Doc = new Document("sku", "mmm").
					append("qty", 5).
					append("price", 2.5);
			Document items = new Document();
			items.put("item1", item1Doc);
			items.put("item2", item2Doc);

			coll.insertOne(new Document("cust_id", "abc123").
					append("ord_date", new Date("Oct 04, 2012")).
					append("status", 'A').
					append("price", 25).
					append("items", items)
					);

			Document book = new Document();
			book.put("name", "Understanding JAVA");
			book.put("pages", 100);
			coll.insertOne(book);

			book = new Document();  
			book.put("name", "Understanding JSON");
			book.put("pages", 200);
			coll.insertOne(book);

			book = new Document();
			book.put("name", "Understanding XML");
			book.put("pages", 300);
			coll.insertOne(book);

			book = new Document();
			book.put("name", "Understanding Web Services");
			book.put("pages", 400);
			coll.insertOne(book);

			book = new Document();
			book.put("name", "Understanding Axis2");
			book.put("pages", 150);
			coll.insertOne(book);

			String map = "function() { "+ 
					"var category; " +  
					"if ( this.pages >= 250 ) "+  
					"category = 'Big Books'; " +
					"else if ( this.pages < 250)" +
					"category = 'Small Books'; "+  
					"emit(category, {name: this.name});}";

			String reduce = "function(key, values) { " +
					"var sum = 0; " +
					"values.forEach(function(doc) { " +
					"sum += 1; "+
					"}); " +
					"return {books: sum};} ";

			//map reduce
			System.out.println("\n Start MapReduce in MongoDB\n");
			MapReduceIterable<Document> iterable = coll.mapReduce(map, reduce);
			MongoCursor<Document> mapReduceCursor = iterable.iterator();
			while(mapReduceCursor.hasNext()) {
				Document mapReduceDoc = mapReduceCursor.next();
				System.out.println(mapReduceDoc.toString());
			}
			System.out.println("\n End MapReduce in MongoDB\n");



			//get all the documents
			FindIterable<Document> findIterable = coll.find();
			MongoCursor<Document> mongoCursor = findIterable.iterator();

			System.out.println("after insert");
			while (mongoCursor.hasNext()) { 
				System.out.println("Inserted Document: "); 
				System.out.println(mongoCursor.next()); 
			}

			//update a document
			Document updateDocument = new Document();
			updateDocument.append("$set", new Document().append("url", "chen-fuxiang.com"));
			coll.updateMany(new Document().append("title", "MongoDB"), updateDocument); 

			//get all the documents
			findIterable = coll.find();
			mongoCursor = findIterable.iterator();

			System.out.println("after update");
			while (mongoCursor.hasNext()) { 
				System.out.println("Inserted Document: "); 
				System.out.println(mongoCursor.next()); 
			}        

			//remove a document

			//Bson condition = new Document("$eq", ObjectId("5709d9671fabd70eec6533ca"));
			//Bson filter = new Document("_id", condition);


			Document removeDocument = new Document();
			removeDocument.append("_id",new ObjectId("5709dbaf1fabd70a882f3e01"));
			coll.deleteOne(removeDocument);

			//get all the documents
			findIterable = coll.find();
			mongoCursor = findIterable.iterator();

			System.out.println("\nafter remove");
			while (mongoCursor.hasNext()) { 
				System.out.println("Inserted Document: "); 
				System.out.println(mongoCursor.next()); 
			}           

		}catch(Exception e){
			System.err.println( e.getClass().getName() + ": " + e.getMessage() );
		}
	}
}