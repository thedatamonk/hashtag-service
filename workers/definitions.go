package workers

import (
	"context"
	"log"
	"regexp"
	"strings"
	"sync"
	"time"

	"github.com/segmentio/kafka-go"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"maps"
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

func bulkUpdateHashtagCounts(db *mongo.Database, hashTagsMapper map[string]int) {
	// get the collection in which you want to insert
	collection := db.Collection("hashtags")
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel() // cancel the context after the function returns	or after the timeout (here 10 seconds) elapses

	var models []mongo.WriteModel
	for tag, count := range hashTagsMapper {
		filter := bson.M{"tag": tag}
		update := bson.M{
			"$inc": bson.M{"count": count},
		}

		model := mongo.NewUpdateOneModel().SetFilter(filter).SetUpdate(update).SetUpsert(true)
		models = append(models, model)
	}

	bulkOptions := options.BulkWrite().SetOrdered(false)
	_, err := collection.BulkWrite(ctx, models, bulkOptions)
	if err != nil {
		log.Printf("Failed to update hashtag counts in bulk: %s\n", err)
	} else {
		log.Printf("Successfully updated hashtag counts in bulk.")
	}

}

func StartNaiveIncrementWorker(db *mongo.Database) {
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



type TagCounter struct {
	mu sync.Mutex
	counts map[string]int	// this map will store the count of each hashtag
	batchSize int			// this indicates the number of messages to be processed before writing to the db
}

// define a method to return an instance of the Tag counter
func NewTagCounter(batchSize int) *TagCounter {
	return &TagCounter{
		counts: make(map[string]int),
		batchSize: batchSize,
	}
}

// define a class method to increment the tag count for the given tag
func (tc *TagCounter) Increment(tag string) {
	// before updating the tag counter mapper, acquire a lock 
	// and then release it after updating the counter
	tc.mu.Lock()
	defer tc.mu.Unlock()
	tc.counts[tag]++
}

// define a class method to reset the counter
func (tc *TagCounter) Reset() {
	// acquire the lock
	tc.mu.Lock()
	defer tc.mu.Unlock()
	tc.counts = make(map[string]int)
}

func (tc *TagCounter) WriteToDB(db *mongo.Database) {
	log.Printf("Now writing to DB.")
	bulkUpdateHashtagCounts(db, tc.counts)
	// reset the counter
	tc.Reset()
	log.Printf("Tag counter values updated in the database and the counter has been reset\n")
}

func StartLocalCopyBatchingWorker(db *mongo.Database) {

	r := kafka.NewReader(kafka.ReaderConfig{
		Brokers: []string{"localhost:9092"},
		Topic: "on_post_publish",
		GroupID: "hashtag-counter",
	})

	// initialise tag counter mapper
	batchSize := 10
	tag_counter := NewTagCounter(batchSize)
	num_processed_messages := 0
	// run the workers infinitely to keep listening of new messages
	for {
		start := time.Now()

		// read the message from the kafka topic
		msg, err := r.ReadMessage(context.TODO())

		if err != nil {
			log.Println("Failed to read message: ", err)
			continue
		}

		num_processed_messages++
		log.Printf("Current # of messages procesed: %d", num_processed_messages)

		// we will still have to extract hashtags from the content of each post
		content := string(msg.Value)
		hashtags := extractHashtags(content)

		// iterate over the hashtags and increment the counter in tag_counter
		for _, tag := range hashtags {
			tag_counter.Increment(tag)
		}

		if num_processed_messages >= batchSize {
			log.Printf("Batch size reached. Writing to DB. %d\n", num_processed_messages)
			num_processed_messages = 0
			// if the number of hashtags in the tag_counter is greater than the batch size
			// then we will update the counts in the database
			// we will reset the tag_counter after updating the counts

			// iterate over all tags and their counts in the tag_counter
			// and update the counts in the database
			tag_counter.WriteToDB(db)
		}
		elapsed := time.Since(start)
		log.Printf("Hashtag counter worker took %s to process all messages\n", elapsed)
	}

}

func createDeepCopy(original map[string]int) map[string]int {
	copy := make(map[string]int)
	maps.Copy(copy, original)
	return copy
}

func StartDeepCopyWorker(db *mongo.Database) {
	r := kafka.NewReader(kafka.ReaderConfig{
		Brokers: []string{"localhost:9092"},
		Topic: "on_post_publish",
		GroupID: "hashtag-counter",
	})

	// initialise tag counter mapper
	batchSize := 10
	tag_counter := NewTagCounter(batchSize)
	num_processed_messages := 0
	// run the workers infinitely to keep listening of new messages
	for {
		start := time.Now()

		// read the message from the kafka topic
		msg, err := r.ReadMessage(context.TODO())

		if err != nil {
			log.Println("Failed to read message: ", err)
			continue
		}

		num_processed_messages++
		log.Printf("Current # of messages procesed: %d", num_processed_messages)

		// we will still have to extract hashtags from the content of each post
		content := string(msg.Value)
		hashtags := extractHashtags(content)

		// iterate over the hashtags and increment the counter in tag_counter
		for _, tag := range hashtags {
			tag_counter.Increment(tag)
		}

		if num_processed_messages >= batchSize {
			log.Printf("Batch size reached. Writing to DB. %d\n", num_processed_messages)
			num_processed_messages = 0
			// if the number of hashtags in the tag_counter is greater than the batch size
			// then we will update the counts in the database
			// we will reset the tag_counter after updating the counts

			// iterate over all tags and their counts in the tag_counter
			// and update the counts in the database
			// create a deep copy of the tag counter counts object
			// and then spin up another thread to write to the database
			
			// ---------- CRITICAL SECTION ------------ //
			tag_counter.mu.Lock()
			copy_tag_counter := createDeepCopy(tag_counter.counts)
			tag_counter.Reset()
			tag_counter.mu.Unlock()
			// ---------- CRITICAL SECTION ------------ //
			
			go bulkUpdateHashtagCounts(db, copy_tag_counter)
		}
		elapsed := time.Since(start)
		log.Printf("Hashtag counter worker took %s to process all messages\n", elapsed)
	}
}


func StartTwoCopiesWorker(db *mongo.Database) {
}

// we will not need any worker here
// this will be a completely different approach
func StartTwoBrokers(db *mongo.Database) {
}