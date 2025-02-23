//	flow of the code
//
// user publishes a post (POST request to post DB) - So this is one endpoint that we need to implemenent
// Now we also have to send a on_publish event to kafka queue
// from this queue hashtag counter worker will be listening to this event and will compute the counts of all hashtags in the hashtag db
// for the purpose of this demo, , we will use mongodb for storing both posts and hashtags
package main

import (
	"context"
	"encoding/json"
	"log"
	"net/http"
	"time"

	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"github.com/segmentio/kafka-go"
	"github.com/thedatamonk/hashtag-service/main/workers"
)

// specify schema of documents in posts and hashtag db
type Post struct {
	ID		primitive.ObjectID	`bson:"_id,omitempty"`
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

func publishPostHandler(w http.ResponseWriter, r *http.Request) {
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

	// send publish post event to kafka
	publishPostEvent(post)

	w.WriteHeader(http.StatusCreated)
	json.NewEncoder(w).Encode(post)

}

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
		Balancer: &kafka.LeastBytes{},
	}
}

func publishPostEvent(post Post) {
	msg := kafka.Message{
		Key: []byte(post.ID.Hex()),
		Value: []byte(post.Content),
	}

	err := kafkaWriter.WriteMessages(context.TODO(), msg)
	if err != nil {
		log.Println("Error publishing post event to Kafka:", err)
	} else {
		log.Println("Successfully published post event to Kafka")
	}
}

	
func main() {
	initDB()
	initKafka()
	go workers.StartWorker(hashtagDB)

	// is the publishPostHandler function running everything inside it synchronously
	http.HandleFunc("/posts/publish", publishPostHandler)
	log.Fatal(http.ListenAndServe(":8080", nil))
}

