package finalTika;
import static com.mongodb.client.model.Filters.*;
import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;

import org.apache.tika.Tika;
import org.bson.Document;

import com.amazonaws.AmazonClientException;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.regions.Region;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.Bucket;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.ListObjectsRequest;
import com.amazonaws.services.s3.model.ObjectListing;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.S3ObjectInputStream;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import com.mongodb.BasicDBObject;
import com.mongodb.DBCursor;
import com.mongodb.MongoClient;
import com.mongodb.MongoClientURI;
import com.mongodb.ServerAddress;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;

public class UnknownFileChecker {
	static final String rootDIR = "/home/roohy/PycharmProjects/CS599_HW1/S3_Downloader";
	static final String dumpDIR = "/home/roohy/PycharmProjects/CS599_HW1/dumped";
	static final String mongoURL = "mongodb://admin:admin123@localhost:27017";
	static final String databaseName = "CS599_HW1";
	static final String POLAR = "polar-fulldump";
	static final int maxSize = 50000;
	
	public static void main(String[] args) throws Exception{
	
		Tika tika = new Tika();
		
		MongoClientURI connectionString = new MongoClientURI(mongoURL);
		MongoClient mongoClient = new MongoClient(connectionString);
		MongoDatabase database = mongoClient.getDatabase(databaseName);
		//database.createCollection("wonders");
		MongoCollection<Document> collection = database.getCollection("files");
		MongoCollection<Document> wonderCollection = database.getCollection("wonders");
		
		AWSCredentials credentials = null;
	        try {
	            credentials = new ProfileCredentialsProvider().getCredentials();
	        } catch (Exception e) {
	        	mongoClient.close();
	            throw new AmazonClientException(
	                    "Cannot load the credentials from the credential profiles file. " +
	                    "Please make sure that your credentials file is at the correct " +
	                    "location (~/.aws/credentials), and is in valid format.",
	                    e);
	        }
	    HashMap<String, Integer> cache = new HashMap<String, Integer>();
		int totalCount = 0;
		AmazonS3 s3 = new AmazonS3Client(credentials);
		S3Object object = null;
		Region usWest2 = Region.getRegion(Regions.US_WEST_1);
        s3.setRegion(usWest2);
        /*
        System.out.println("Listing buckets");
        for (Bucket bucket : s3.listBuckets()) {
            System.out.println(" - " + bucket.getName());
        }*/
		
        FindIterable<Document> cursor = null;
        String wonderType = "";

        BasicDBObject query = new BasicDBObject("type","application/octet-stream");
        cursor = collection.find();
        MongoCursor<Document> mongoCursor = cursor.iterator();
        Document doc = null;
        
        try{
        while(mongoCursor.hasNext()){
        	doc = mongoCursor.next();
        	if(!doc.get("type").equals("application/octet-stream")){
        		//System.out.println("something is wrong"+doc.get("type"));
        		continue;
        	}
        	object = s3.getObject(new GetObjectRequest(POLAR, doc.getString("key")));
        	S3ObjectInputStream inputStream = object.getObjectContent();
        	wonderType = tika.detect(inputStream);
        	System.out.println("Scanned "+totalCount+" items...");
        	if(wonderType != "application/octet-stream"){
        		if(cache.containsKey(wonderType)){
        			cache.put(wonderType, cache.get(wonderType)+1);
            	}else{
            		cache.put(wonderType, 1);
            	}
        		Document wonderDoc = new Document("key",doc.getString("key"));
        		wonderDoc.append("type", wonderType);
        		wonderCollection.insertOne(wonderDoc);
        		System.out.println("Wonder happened");
        	}
        	totalCount += 1;
        }
        }
        catch(Exception e){
        	e.printStackTrace();
        }
        finally{
        	System.out.println("some error happened or not :D");
        	for( String e : cache.keySet()){
    			System.out.println(e+" : "+cache.get(e));
        	}
        }
		
		for( String e : cache.keySet()){
			System.out.println(e+" : "+cache.get(e));
			
		}
		mongoClient.close();
	}
}
