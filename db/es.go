package db

import (
	"context"
	"errors"
	"fmt"
	"log"
	"os"
	"strconv"

	"github.com/tomekwlod/utils"
	elastic "gopkg.in/olivere/elastic.v6"
)

type esConfig struct {
	Addr       string
	Port       int
	UseSniffer bool
}

func Client() (client *elastic.Client, err error) {
	ec := esConfig{"localhost", 9202, false}

	// Create ES client here; If no connection - nothing to do here
	client, err = newESClient(ec)
	if err != nil {
		return
	}

	// Create mapping
	err = createIndex(client, "experts")
	if err != nil {
		fmt.Println(err) //just temp - remove me later
		return
	}

	fmt.Println("Connection to ElasticServer established")

	return
}

func newESClient(ec esConfig) (client *elastic.Client, err error) {
	// not sure
	errorlog := log.New(os.Stdout, "ESAPP ", log.LstdFlags)

	// ip plus port plus protocol
	addr := "http://" + ec.Addr + ":" + strconv.Itoa(ec.Port)

	var configs []elastic.ClientOptionFunc
	configs = append(configs, elastic.SetURL(addr), elastic.SetErrorLog(errorlog))
	configs = append(configs, elastic.SetSniff(ec.UseSniffer)) // this is very important when you use proxy above your ES instance; it may be though wanted for many ES nodes

	// Obtain a client. You can also provide your own HTTP client here.
	client, err = elastic.NewClient(configs...)
	if err != nil {
		return
	}

	// Trace request and response details like this
	//client.SetTracer(log.New(os.Stdout, "", 0))

	// Ping the Elasticsearch server to get info, code, and error if any
	_, _, err = client.Ping(addr).Do(context.Background())
	if err != nil {
		return
	}

	// Getting the ES version number is quite common, so there's a shortcut
	_, err = client.ElasticsearchVersion(addr)
	if err != nil {
		return
	}

	return
}

func createIndex(client *elastic.Client, index string) (err error) {
	// Use the IndexExists service to check if a specified index exists.
	exists, err := client.IndexExists(index).Do(context.Background())
	if err != nil {
		return
	}

	if exists {
		return
	}

	fmt.Println("No mapping found. Creating one")

	// Create a new index
	file, err := utils.ReadWholeFile("./mapping.json")
	if err != nil {
		return
	}

	ic, err := client.CreateIndex(index).Body(string(file)).Do(context.Background())
	if err != nil {
		return
	}

	if !ic.Acknowledged {
		err = errors.New("Mapping couldn't be acknowledged")
		return
	}

	return
}
