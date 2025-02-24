// user publishes a post (POST request to post DB) - So this is one endpoint that we need to implemenent
// Now we also have to send a on_publish event to kafka queue
// from this queue hashtag counter worker will be listening to this event and will compute the counts of all hashtags in the hashtag db
// for the purpose of this demo, , we will use mongodb for storing both posts and hashtags
package main

import (
	"context"
	"encoding/json"
	"flag"
	"log"
	"net/http"
	"time"

	"github.com/segmentio/kafka-go"
	"github.com/thedatamonk/hashtag-service/main/workers"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)



// specify schema of documents in posts and hashtag db
type Post struct {
	ID		primitive.ObjectID	`bson:"_id,omitempty"`
	UserName  string 			`bson:"username"`
	Content		string			`bson:"content"`
	CreatedAt	time.Time			`bson:"created_at"`
}

type Hashtag struct {
	ID		primitive.ObjectID	`bson:"_id,omitempty"`
	Tag		string			`bson:"tag"`
	Count		int			`bson:"count"`
}

var postDB *mongo.Database
var hashtagDB *mongo.Database


// Approach 1: Naive Increment Counter
// In this approach, we will increment the counter of each hashtag in the database
func publishPostHandler(w http.ResponseWriter, r *http.Request) {
	start := time.Now()

	var post Post
	err := json.NewDecoder(r.Body).Decode(&post)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	post.ID = primitive.NewObjectID()
	post.CreatedAt = time.Now()

	// create a collection called posts inside postDB
	collection := postDB.Collection("posts")
	// insert the newly created post in the post colection
	_, err = collection.InsertOne(context.TODO(), post)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	// send publish post event to kafka in a separate go-routine
	// to reduce the perceived time taken to publish the post
	go publishPostEvent(post)

	w.WriteHeader(http.StatusCreated)
	json.NewEncoder(w).Encode(post)

	elapsed := time.Since(start)
	log.Printf("publishPostHandler took %s", elapsed)

}


// Approach 2: we will locally keep a copy of the counters of each tag
// and only after a certain time will write it to mongodb
// a local copy can be kept in a hashmap


// Approach 3: in this approach, we will create a deep copy of the hashmap
// and then use this deep copy to write into the db
// after creating a copy we will clear the original map and then start consuming messages again


// Approach 4: we maintain two maps in this approach - active and passive
// the messages are currently being written to the active map
// when it's full, we will swap the maps and start writing to the passive map
// ma, mp = mp, ma
// So now mp(ma) is being used to update the DB
// while ma(mp) is being used to collect the messages
// this way we can avoid locking the map for a longer time



// Approach 5: in this approach, we will have two brokers
// one broker (partitioned by user_id) will be responsible for receiving posts and extracting hashtags per post
// for each hashtag extracted from the post, send another event to  the other broker (parttitioned by hashtag)
// the consumer of this broker will accumulate the number of posts per hashtag and then write it in bulk to mongodb


func initDB() {
	// specify client options
	clientOptions := options.Client().ApplyURI("mongodb://localhost:27017")

	// connect to mongodb instance with the given client options
	client, err := mongo.Connect(context.TODO(), clientOptions)

	if err != nil {
		log.Fatal(err)
	}

	// if everything is fine, once ping the connection
	err = client.Ping(context.TODO(), nil)
	if err != nil {
		log.Fatal(err)
	}

	postDB = client.Database("postdb")
	hashtagDB = client.Database("hashtagdb")

	log.Println("Connected to MongoDB!")
	log.Println("postDB:", postDB.Name())
	log.Println("hashtagDB:", hashtagDB.Name())
}

var kafkaWriter *kafka.Writer

func initKafka() {
	// create a new kafka writer object to send kafka events to the specified topic
	kafkaWriter = &kafka.Writer{
		Addr: kafka.TCP("localhost:9092"),
		Topic: "on_post_publish",
		Balancer: &kafka.Hash{},
	}
}

func publishPostEvent(post Post) {
	start := time.Now()
	msg := kafka.Message{
		Key: []byte(post.UserName),
		Value: []byte(post.Content),
	}

	err := kafkaWriter.WriteMessages(context.TODO(), msg)
	if err != nil {
		log.Println("Error publishing post event to Kafka:", err)
	} else {
		log.Println("Successfully published post event to Kafka")
	}

	elapsed := time.Since(start)
	log.Printf("publishPostEvent took %s", elapsed)
}


var approach_type string

func init() {
	flag.StringVar(&approach_type, "approach-type", "naive", "Type of approach to apply for incrementing hashtag counter")
}
	
func main() {
	// parse command line arguments
	flag.Parse()

	// initialise MongoDB and Kafka
	initDB()
	initKafka()

	// depending upon the approach type, start the corresponding worker

	if approach_type == "naive" {
		log.Printf("Approach Type: %s", approach_type)
		go workers.StartNaiveIncrementWorker(hashtagDB)

	} else if approach_type == "naive_local_batching" {
		log.Printf("Approach Type: %s", approach_type)
		go workers.StartLocalCopyBatchingWorker(hashtagDB)
	
	} else if approach_type == "optimised_deep_copy" {
		log.Printf("Approach Type: %s", approach_type)
		go workers.StartDeepCopyWorker(hashtagDB)
		
	} else if approach_type == "optimised_two_copies" {
		log.Printf("Approach Type: %s", approach_type)
		go workers.StartTwoCopiesWorker(hashtagDB)

	} else if approach_type == "optimised_two_brokers" {
		log.Printf("Approach Type: %s", approach_type)
		go workers.StartLocalCopyBatchingWorker(hashtagDB)

	} else {
		log.Fatalf("Unknown approach type: %s", approach_type)
	}

	http.HandleFunc("/posts/publish", publishPostHandler)
	log.Fatal(http.ListenAndServe(":8080", nil))
}

