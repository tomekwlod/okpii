package models

import (
	"context"
	"fmt"
	"os"
	"time"

	mongo "github.com/mongodb/mongo-go-driver/mongo"
	"github.com/mongodb/mongo-go-driver/mongo/readpref"
)

type Repository interface {
	Onekeys(out chan<- map[string]string)
	CountOneKeyOcc(custName, fn, ln string) int64
	IsInOneKeyDB(fn, mn, ln string) bool

	Flush(operations []mongo.WriteModel) error
}

type DB struct {
	*mongo.Database
}

func MongoDB() (*DB, error) {
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
		return nil, err
	}

	ctx, _ := context.WithTimeout(context.Background(), 10*time.Second)
	err = client.Connect(ctx)
	if err != nil {
		return nil, err
	}

	ctx, _ = context.WithTimeout(context.Background(), 2*time.Second)
	err = client.Ping(ctx, readpref.Primary())
	if err != nil {
		return nil, err
	}

	fmt.Println("Connection to MongoDB established")

	db := client.Database(dbname)

	return &DB{db}, nil
}
