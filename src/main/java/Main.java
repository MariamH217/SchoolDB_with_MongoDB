import com.mongodb.client.*;
import com.mongodb.client.model.*;
import com.mongodb.client.result.DeleteResult;
import com.mongodb.client.result.UpdateResult;
import org.bson.Document;
import org.bson.conversions.Bson;

import java.util.*;

public class Main {
    private static MongoClient mongoClient;
    private static MongoDatabase database;

    static void main(String[] args) {
        String connectionString = "mongodb://localhost:27017";

        try {
            mongoClient = MongoClients.create(connectionString);
            database = mongoClient.getDatabase("SchoolDB");
            System.out.println("Successfully connected to database");
            menu();

        } catch (Exception e) {
            System.err.println("Error: " + e.getMessage());
        }
    }

    static void insertStudentIntoCollection(MongoDatabase database) {
        try {
            MongoCollection<Document> collection = database.getCollection("Students");
            Scanner scanner = new Scanner(System.in);

            System.out.print("Enter student name: ");
            String name = scanner.nextLine();

            System.out.print("Enter student age: ");
            int age = scanner.nextInt();
            scanner.nextLine();

            System.out.print("Enter student ID: ");
            int studentId = scanner.nextInt();
            scanner.nextLine();


            List<Integer> courseIdList = new ArrayList<>();
            System.out.println("Enter enrolled course IDs (enter -1 to finish):");
            int courseId;
            while ((courseId = scanner.nextInt()) != -1) {
                courseIdList.add(courseId);
            }
            scanner.nextLine();

            Document document = new Document();
            document.put("name", name);
            document.put("age", age);
            document.put("studentId", studentId);
            document.put("enrolledCourses", courseIdList);
            collection.insertOne(document);

            System.out.println("Document inserted successfully.");
        } catch (Exception e) {
            System.err.println("Error: " + e.getMessage());
        }
    }

    static void findStudentsByName(MongoDatabase database) {
        try {
            MongoCollection<Document> collection = database.getCollection("Students");
            Scanner scanner = new Scanner(System.in);

            System.out.print("Enter student name to search: ");
            String name = scanner.nextLine();

            Document query = new Document("name", name);
            FindIterable<Document> cursor = collection.find(query);

            for (Document document : cursor) {
                System.out.println(document.toJson());
            }
        } catch (Exception e) {
            System.err.println("Error: " + e.getMessage());
        }
    }

    static void updateStudentDetails(MongoDatabase database) {
        try {
            MongoCollection<Document> collection = database.getCollection("Students");

            Scanner scanner = new Scanner(System.in);
            System.out.print("Enter student ID to update: ");
            int studentId = scanner.nextInt();
            scanner.nextLine();

            System.out.print("Enter new name: ");
            String newName = scanner.nextLine();

            System.out.print("Enter new age: ");
            int newAge = scanner.nextInt();
            scanner.nextLine();

            // Create the update operation
            Bson filter = Filters.eq("studentId", studentId);
            Bson updateName = Updates.set("name", newName);
            Bson updateAge = Updates.set("age", newAge);

            Bson updateOperation = Updates.combine(updateName, updateAge);

            UpdateResult updateResult = collection.updateOne(filter, updateOperation);

            if (updateResult.getModifiedCount() > 0) {
                System.out.println("Student details updated successfully.");
            } else {
                System.out.println("No student found with the specified studentId.");
            }
        } catch (Exception e) {
            System.err.println("Error: " + e.getMessage());
        }
    }

    static void deleteStudent(MongoDatabase database) {
        try {
            MongoCollection<Document> collection = database.getCollection("Students");

            Scanner scanner = new Scanner(System.in);
            System.out.print("Enter student ID to delete: ");
            int studentId = scanner.nextInt();
            scanner.nextLine();

            Bson filter = Filters.eq("studentId", studentId);

            DeleteResult deleteResult = collection.deleteOne(filter);

            if (deleteResult.getDeletedCount() > 0) {
                System.out.println("Student with studentId " + studentId + " deleted successfully.");
            } else {
                System.out.println("No student found with the specified studentId.");
            }
        } catch (Exception e) {
            System.err.println("Error: " + e.getMessage());
        }
    }

    static void findStudentsEnrolledInCourse(MongoDatabase database) {
        try {
            MongoCollection<Document> collection = database.getCollection("Students");

            Scanner scanner = new Scanner(System.in);
            System.out.print("Enter course ID to search: ");
            int courseId = scanner.nextInt();
            scanner.nextLine();

            Bson filter = Filters.eq("enrolledCourses", courseId);

            Bson projection = Projections.fields(
                    Projections.include("name", "studentId"),
                    Projections.excludeId()
            );

            FindIterable<Document> result = collection.find(filter).projection(projection);

            System.out.println("Students enrolled in course " + courseId + ":");
            for (Document doc : result) {
                System.out.println(doc.toJson());
            }
        } catch (Exception e) {
            System.err.println("Error: " + e.getMessage());
        }
    }

    static void addNewCourseToStudent(MongoDatabase database) {
        try {
            MongoCollection<Document> collection = database.getCollection("Students");
            Scanner scanner = new Scanner(System.in);

            System.out.print("Enter student ID: ");
            int studentId = scanner.nextInt();
            scanner.nextLine();

            System.out.print("Enter new course ID to add: ");
            int newCourseId = scanner.nextInt();
            scanner.nextLine();

            Bson filter = Filters.eq("studentId", studentId);
            Bson update = Updates.addToSet("enrolledCourses", newCourseId);

            UpdateResult updateResult = collection.updateOne(filter, update);

            if (updateResult.getModifiedCount() > 0) {
                System.out.println("New course added to student's enrolledCourses successfully.");
            } else {
                System.out.println("No student found with the specified studentId.");
            }
        } catch (Exception e) {
            System.err.println("Error: " + e.getMessage());
        }
    }

    static void calculateAverageAgeInCourse(MongoDatabase database) {
        try {
            MongoCollection<Document> collection = database.getCollection("Students");

            Scanner scanner = new Scanner(System.in);
            System.out.print("Enter course ID to calculate average age: ");
            int courseId = scanner.nextInt();
            scanner.nextLine();

            List<Bson> pipeline = Arrays.asList(
                    Aggregates.match(Filters.eq("enrolledCourses", courseId)), // Filter documents with specified courseId
                    Aggregates.group(null, Accumulators.avg("averageAge", "$age")) // Group documents and calculate average age
            );

            AggregateIterable<Document> result = collection.aggregate(pipeline);

            Document resultDocument = result.first();
            if (resultDocument != null) {
                double averageAge = resultDocument.getDouble("averageAge");
                System.out.println("Average age in course " + courseId + ": " + averageAge);
            } else {
                System.out.println("No students found enrolled in course " + courseId);
            }
        } catch (Exception e) {
            System.err.println("Error: " + e.getMessage());
        }
    }

    static void listAllCourses(MongoDatabase database) {
        try {
            MongoCollection<Document> collection = database.getCollection("Courses");

            FindIterable<Document> result = collection.find().projection(Projections.excludeId());

            for (Document course : result) {
                System.out.println(course.toJson());
            }
        } catch (Exception e) {
            System.err.println("Error: " + e.getMessage());
        }
    }

    static void findNumberOfStudentsInEachCourse() {
        try {
            MongoCollection<Document> collection = database.getCollection("Students");

            List<Bson> pipeline = Arrays.asList(
                    Aggregates.unwind("$enrolledCourses"), // Unwind the enrolledCourses array
                    Aggregates.group("$enrolledCourses", Accumulators.sum("count", 1)) // Group by enrolledCourses and count the number of occurrences
            );

            AggregateIterable<Document> result = collection.aggregate(pipeline);

            System.out.println("Number of students in each course:");
            for (Document doc : result) {
                int courseId = doc.getInteger("_id");
                int count = doc.getInteger("count");
                System.out.println("Course ID: " + courseId + ", Number of Students: " + count);
            }
        } catch (Exception e) {
            System.err.println("Error: " + e.getMessage());
        }
    }

    static void manuOptions() {

        System.out.println("0. Exit the program");
        System.out.println("1. Add Student");
        System.out.println("2. Update student details ");
        System.out.println("3. Delete student");
        System.out.println("4. Find student by name");
        System.out.println("5. Add new course to student");
        System.out.println("6. Number of students in a course");
        System.out.println("7. The average age in a course");
        System.out.println("8. List students in a course ");
        System.out.println("9. List all courses ");

    }

    static void menu() {
        int answer;

        do {
            manuOptions();
            Scanner sc = new Scanner(System.in);
            answer = sc.nextInt();

            switch (answer) {
                case 0:
                    System.out.println("Exiting the program. Goodbye!");
                    break;
                case 1:
                    insertStudentIntoCollection(database);
                    break;
                case 2:
                    updateStudentDetails(database);
                    break;
                case 3:
                    deleteStudent(database);
                    break;
                case 4:
                    findStudentsByName(database);
                    break;
                case 5:
                    addNewCourseToStudent(database);
                    break;
                case 6:
                    findNumberOfStudentsInEachCourse();
                    break;
                case 7:
                    calculateAverageAgeInCourse(database);
                    break;
                case 8:
                    findStudentsEnrolledInCourse(database);
                    break;
                case 9:
                    listAllCourses(database);
                    break;
                default:
                    System.out.println("Invalid option. Please choose a valid option.");
            }
        } while (answer != 0);
    }
}
