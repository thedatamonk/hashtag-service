package workers

import (
	"context"
	"log"
	"regexp"
	"strings"
	"time"

	"github.com/segmentio/kafka-go"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)


func extractHashtags(content string) []string {
	// tnis function will extract hashtags from the content of the post
	re := regexp.MustCompile(`#\w+`)
	return re.FindAllString(content, -1)
}


func updateHashtagCounts(db *mongo.Database, hashTags []string) {
	// this function will update the count of the hashtags in the database
	// get the hashtags collection from the hashtag DB in mongodb
	collection := db.Collection("hashtags")

	// iterate over the hashtags and update the count in the collection
	for _, tag := range hashTags {
		tag = strings.ToLower(tag)
		filter := bson.M{"tag": tag}
		update := bson.M{"$inc": bson.M{"count": 1}}
		_, err := collection.UpdateOne(context.TODO(), filter, update, options.Update().SetUpsert(true))
		if err != nil {
			log.Printf("Failed to update hashtag count of tag %s: \t Error: %s\n", tag, err)
		}
	}

	log.Printf("Successfully updated hashtag counts for %d hashtags\n", len(hashTags))

}


func StartWorker(db *mongo.Database) {
	// main function that will be called by the main thread to start the workers in a
	// separate thread
	// create a new reader worker
	r := kafka.NewReader(kafka.ReaderConfig{
		Brokers: []string{"localhost:9092"},
		Topic: "on_post_publish",
		GroupID: "hashtag-counter",
	})


	// run the workers infinitely to keep listening of new messages
	for {
		start := time.Now()

		msg, err := r.ReadMessage(context.TODO())
		if err != nil {
			log.Println("Failed to read message: ", err)
			continue
		}

		content := string(msg.Value)

		hashtags := extractHashtags(content)
		updateHashtagCounts(db, hashtags)
		elapsed := time.Since(start)
		log.Printf("Hashtag counter worker took %s to process all messages\n", elapsed)
	}

}