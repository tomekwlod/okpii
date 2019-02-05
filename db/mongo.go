package db

import (
	"context"
	"fmt"
	"os"
	"time"

	mongo "github.com/mongodb/mongo-go-driver/mongo"
	"github.com/mongodb/mongo-go-driver/mongo/readpref"
)

func MongoDB() (database *mongo.Database, err error) {
	host := os.Getenv("MONGO_HOST")
	if host == "" {
		host = "localhost"
	}
	port := os.Getenv("MONGO_PORT")
	if port == "" {
		port = "27017"
	}
	dbname := os.Getenv("MONGO_DB")
	if dbname == "" {
		dbname = "database"
	}

	client, err := mongo.NewClient("mongodb://" + host + ":" + port)
	if err != nil {
		return
	}

	ctx, _ := context.WithTimeout(context.Background(), 10*time.Second)
	err = client.Connect(ctx)
	if err != nil {
		return
	}

	ctx, _ = context.WithTimeout(context.Background(), 2*time.Second)
	err = client.Ping(ctx, readpref.Primary())
	if err != nil {
		return
	}

	fmt.Println("Connection to MongoDB established")

	database = client.Database(dbname)

	return
}
