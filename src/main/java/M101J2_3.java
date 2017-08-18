import com.mongodb.MongoClient;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;
import org.bson.Document;
import org.bson.types.ObjectId;

import static com.mongodb.client.model.Sorts.*;
import static com.mongodb.client.model.Filters.*;



public class M101J2_3 {

    public static void main(String[] args) {
        MongoClient mongoClient = new MongoClient();
        MongoDatabase database = mongoClient.getDatabase("students");
        final MongoCollection<Document> collection = database.getCollection("grades");
        MongoCursor<Document> cursor = collection.find(
                eq("type", "homework")).sort(descending("student_id", "score")).iterator();
        Document lastDocument = null;
        //Document nextDocument;
        while (cursor.hasNext()) {
            Document document = cursor.next();
            Integer lastStudent = null;
            if (lastDocument != null) {
                lastStudent = lastDocument.getInteger("student_id");
            }
            Integer currentStudent = document.getInteger("student_id");

            if(lastDocument != null && !lastStudent.equals(currentStudent)) {

                ObjectId id = (ObjectId)lastDocument.get("_id");
                System.out.println("Deleting " + id.toString());
                collection.deleteOne(eq("_id", id));
            }
            lastDocument = document;
        }
        ObjectId id = (ObjectId)lastDocument.get("_id");
        System.out.println("Deleting " + id.toString());
        collection.deleteOne(eq("_id", id));
    }

}
