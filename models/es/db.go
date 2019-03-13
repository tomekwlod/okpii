package models

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"os"
	"path"
	"strconv"

	modelsMysql "github.com/tomekwlod/okpii/models/mysql"
	"github.com/tomekwlod/utils"
	elastic "gopkg.in/olivere/elastic.v6"
)

const mappingfn = "mapping.json"

type Repository interface {
	// searches
	SimpleSearch(id, custName, fn, mn, ln, city string, did int, exclIDs []string) []map[string]interface{}
	ShortSearch(id, custName, fn, mn, ln, city string, did int, exclIDs []string) []map[string]interface{}
	NoMiddleNameSearch(id, custName, fn, mn, ln, city string, did int, exclIDs []string) []map[string]interface{}
	OneMiddleNameSearch(id, custName, fn, mn, ln, city string, did int, exclIDs []string) []map[string]interface{}
	OneMiddleNameSearch2(id, custName, fn, mn, ln, city string, did int, exclIDs []string) []map[string]interface{}
	TestSearch(id, custName, fn, mn, ln, city string, did int, exclIDs []string) []map[string]interface{}

	// index
	IndexExperts(experts []*modelsMysql.Experts, batchInsert int) error
}

type DB struct {
	*elastic.Client
}

type esConfig struct {
	Addr       string
	Port       int
	UseSniffer bool
}

func ESClient() (*DB, error) {
	host := os.Getenv("ES_HOST")
	if host == "" {
		host = "localhost"
	}
	port := os.Getenv("ES_PORT")
	if port == "" {
		port = "9202"
	}
	iport, _ := strconv.Atoi(port)

	ec := esConfig{host, iport, false}

	// Create ES client here; If no connection - nothing to do here
	db, err := newESClient(ec)
	if err != nil {
		return nil, err
	}

	// Create mapping
	err = createIndex(db, "experts")
	if err != nil {
		return nil, err
	}

	fmt.Println("Connection to ElasticServer established")

	return &DB{db}, nil
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

	if os.Getenv("STATICPATH") == "" {
		// in prod mode (with the docker) the STATICPATH won't be empty

		// in dev mode set the default static path
		os.Setenv("STATICPATH", "../../data/static")
	}
	filename := path.Join(os.Getenv("STATICPATH"), mappingfn)

	// Create a new index
	file, err := utils.ReadWholeFile(filename)
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

func PrintESQuery(nss *elastic.SearchSource) {
	sjson, err := nss.Source()
	if err != nil {
		panic(err)
	}
	data, err := json.MarshalIndent(sjson, "", "  ")
	if err != nil {
		panic(err)
	}

	log.Printf("%s\n", string(data))
}
