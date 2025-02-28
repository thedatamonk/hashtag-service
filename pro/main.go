package main

import (
	"context"
	"encoding/json"
	"log"
	"net/http"
	"regexp"
	"time"

	"github.com/segmentio/kafka-go"
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


// var postDB, hashtagDB *mongo.Database
var reader1, reader2 *kafka.Reader
var writer1, writer2 *kafka.Writer


var postDB, hashtagDB *mongo.Database

func extractHashtags(content string) []string {
	// tnis function will extract hashtags from the content of the post
	re := regexp.MustCompile(`#\w+`)
	return re.FindAllString(content, -1)
}

func createKafkaReader(addr string, topic string, group_id string) *kafka.Reader {
	defer log.Printf("Created Kafka Reader @ %s for topic: %s and GroupID: %s", addr, topic, group_id)
	return kafka.NewReader(kafka.ReaderConfig{
        Brokers: []string{addr},
        Topic:   topic,
        GroupID: group_id,
    })
}

func createKafkaWriter(addr string, topic string) *kafka.Writer {
	defer log.Printf("Created Kafka Writer @ %s for topic: %s", addr, topic)
	return kafka.NewWriter(kafka.WriterConfig{
        Brokers: []string{addr},
        Topic:   topic,
        Balancer: &kafka.Hash{},
    })
}

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
	go createPostEvent(post)

	w.WriteHeader(http.StatusCreated)
	json.NewEncoder(w).Encode(post)
	
	elapsed := time.Since(start)
	log.Printf("publishPostHandler took %s", elapsed)
	
}

func createPostEvent(post Post) {
	start := time.Now()
	
	// each message has the UserName has key and this will be used as partition key
	msg := kafka.Message{
		Key: []byte(post.UserName),
		Value: []byte(post.Content),
	}

	err := writer1.WriteMessages(context.TODO(), msg)
	if err != nil {
		log.Println("Error publishing post event to Kafka:", err)
	} else {
		log.Println("Successfully published post event to Kafka")
	}

	elapsed := time.Since(start)
	log.Printf("createPostEvent took %s", elapsed)

}

func extractTags(reader *kafka.Reader, writer *kafka.Writer) {
	// R1 ==> W2
	for {

		start := time.Now()
		// Read messages from the first broker
        msg, err := reader.ReadMessage(context.TODO())
		if err != nil {
            log.Println("failed to read message:", err)
			continue
        }

		post_content := string(msg.Value)
		hashtags := extractHashtags(post_content) 

		for _, tag := range hashtags {
			// send a on_tag_extracted event
			msg = kafka.Message{
					Key:[]byte(tag), // You can choose to keep the same key or generate a new one
					Value: []byte("1"),
				}
			err = writer.WriteMessages(context.TODO(), msg)
						
			if err != nil {
				log.Println("Failed to write `on_tag_extracted` event to second broker:", err)
			} else {
				log.Println("Successfully published `on_tag_extracted` event to Kafka")
			}

		}
		
		elapsed := time.Since(start)
		log.Printf("extractTags worker took %s to process all tags\n", elapsed)

	}

}

func readTagExtractedEvent(reader *kafka.Reader) {
	// R2
	for {

		start := time.Now()

        msg, err := reader.ReadMessage(context.TODO())
        if err != nil {
            log.Println("Failed to read `on_tag_extracted` event from second broker:", err)
			continue
        }

		tag := string(msg.Key)

		// TODO: now update the count of this tag in the local buffer
		log.Printf("Processing %s\n", tag)
		elapsed := time.Since(start)
		log.Printf("`readTagExtractedEvent` finished in %s\n", elapsed)
    }

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


func main() {

	// create 2 writers
	// W1 => Writes the `on_publish_post` event to Broker 1
	// W2 => Writes the `on_tag_extracted` event to Broker 2
	
	// create 2 readers
	// R1 => Reads from Broker 1 the `on_publish_post` event, then extracts tags
	// For each tag, calls worker W2 to write the `on_tag_extracted` event to Broker 2
	// 
	// R2 => Reads the `on_tag_extracted` event from Broker 2, then updates the local counter buffer
	// And also periodically writes into mongodb

	initDB()
	
	reader1 = createKafkaReader("localhost:9092", "posts", "posts_group")
	reader2 = createKafkaReader("localhost:9094", "tags", "tags_group")

	defer reader1.Close()
	defer reader2.Close()

	writer1 = createKafkaWriter("localhost:9092", "posts")
	writer2 = createKafkaWriter("localhost:9094", "tags")

	defer writer1.Close()
	defer writer2.Close()

	go extractTags(reader1, writer2)

	go readTagExtractedEvent(reader2)

	// create an endpoint that will be invoked when the user publishes a post
	http.HandleFunc("/posts/publish", publishPostHandler)
	log.Fatal(http.ListenAndServe(":8080", nil))
}