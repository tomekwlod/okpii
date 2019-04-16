package models

import (
	"context"
	"fmt"
	"time"

	"github.com/mongodb/mongo-go-driver/bson"
	"github.com/mongodb/mongo-go-driver/bson/primitive"
	mongo "github.com/mongodb/mongo-go-driver/mongo"
	"github.com/mongodb/mongo-go-driver/mongo/options"
)

func (db *DB) ClearCollection() (rowsAffected int64, err error) {
	// defining the collection
	collection := db.Collection("test2")

	filter := bson.M{}

	delres, err := collection.DeleteMany(context.TODO(), filter)
	if err != nil {
		// no match, it is truly unique
		return 0, err
	}

	// entry found, skip!
	return delres.DeletedCount, nil
}

func (db *DB) IsInOneKeyDB(fn, mn, ln string) bool {
	// defining the collection
	collection := db.Collection("test2")

	if mn != "" {
		// todo!! original name should come instead!! because of the middle names issue
	}

	filter := bson.M{
		"FIRST_NAME": fn,
		"LAST_NAME":  ln,
	}

	// Pass these options to the Find method
	options := options.FindOne()
	options.SetProjection(bson.D{
		{"_id", 1},
	})

	var elem map[string]string
	err := collection.FindOne(context.TODO(), filter, options).Decode(&elem)
	if err != nil {
		// no match, it is truly unique
		return true
	}

	// entry found, skip!
	return false
}

func (db *DB) CountOneKeyOcc(custName, fn, ln string) int64 {
	// defining the collection
	collection := db.Collection("test2")

	var ifn interface{}
	ifn = fn
	if len(fn) == 1 {
		ifn = primitive.Regex{Pattern: "^" + fn + ".*", Options: ""}
	}

	filter := bson.D{
		{"FIRST_NAME", ifn},
		{"LAST_NAME", ln},
		{"CUST_NAME", bson.D{
			{"$ne", custName},
		}},
	}

	// Pass these options to the Find method
	// options := options.Count()

	counter, err := collection.Count(context.TODO(), filter, nil)

	if err != nil {
		// no match, it is truly unique
		panic(err)
	}

	// entry found, skip!
	return counter
}

func (db *DB) Onekeys(out chan<- map[string]string) {
	defer close(out)

	// defining the collection
	collection := db.Collection("test2")

	// filter := bson.D{{"SRC_CUST_ID", "WDEM01690729"}}
	filter := bson.D{{}}

	// Pass these options to the Find method
	options := options.Find()
	options.SetProjection(bson.D{
		{"_id", 0},
		{"FIRST_NAME", 1},
		{"LAST_NAME", 1},
		{"CUST_NAME", 1},
		{"SRC_CUST_ID", 1},
		{"CITY", 1},
		{"CNTRY", 1},
	})
	// options.SetLimit(10)

	ctx, cancel := context.WithTimeout(context.Background(), 6*600*time.Second) // 10min * 6 = 1h
	defer cancel()
	cur, err := collection.Find(ctx, filter, options)
	if err != nil {
		panic(err)
	}

	// Finding multiple documents returns a cursor
	// Iterating through the cursor allows us to decode documents one at a time
	for cur.Next(context.TODO()) {
		// create a value into which the single document can be decoded
		var elem map[string]string

		err := cur.Decode(&elem)
		if err != nil {
			panic(err)
		}

		out <- elem
	}

	if err := cur.Err(); err != nil {
		panic(err)
	}

	// Close the cursor once finished
	cur.Close(context.TODO())
}

func (db *DB) Flush(operations []mongo.WriteModel) (err error) {
	t1 := time.Now()

	// defining the collection
	collection := db.Collection("test2")

	ctx, cancel := context.WithTimeout(context.Background(), 600*time.Second) // 10min
	defer cancel()
	bwr, err := collection.BulkWrite(ctx, operations)
	if err != nil {
		return
	}

	t2 := time.Now()

	fmt.Printf("Inserted: %d, Upserted %d, Modified: %d, Matched: %d, \nDone in: %v \n",
		bwr.InsertedCount, bwr.UpsertedCount, bwr.ModifiedCount, bwr.MatchedCount, t2.Sub(t1),
	)

	return
}
