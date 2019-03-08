package main

/*
DUMP
Dumps data from MySQL to Elasticsearch
*/

import (
	"fmt"
	"strconv"

	"github.com/tomekwlod/okpii/internal"
	modelsES "github.com/tomekwlod/okpii/models/es"
	modelsMysql "github.com/tomekwlod/okpii/models/mysql"

	_ "github.com/go-sql-driver/mysql"
	_ "golang.org/x/net/html/charset"
)

const batchInsert = 2000

type service struct {
	es    modelsES.Repository
	mysql modelsMysql.Repository
	// logger  *log.Logger
}

type Experts struct {
	ID                int      `json:"id"`
	Did               int      `json:"did"`
	Deleted           int      `json:"deleted"`
	NPI               int      `json:"npi"`
	TTID              int      `json:"ttid"`
	Position          int      `json:"position"`
	Name              string   `json:"name"`
	NameKeyword       string   `json:"nameKeyword"`
	NameKeywordSquash string   `json:"nameKeywordSquash"`
	NameKeywordRaw    string   `json:"nameKeywordRaw"`
	Fn                string   `json:"fn"`
	Mn                string   `json:"mn"`
	Ln                string   `json:"ln"`
	FNDash            bool     `json:"fnDash"`
	FNDot             bool     `json:"fnDot"`
	Country           string   `json:"country"`
	City              string   `json:"city"`
	Aliases           []string `json:"aliases"`
}

func main() {

	deployments, err := internal.Deployments()
	if err != nil {
		panic(err)
	}
	fmt.Printf("\nStarting with: %v deployment(s)\n\n", deployments)

	esClient, err := modelsES.ESClient()
	checkErr(err)

	mysqlClient, err := modelsMysql.MysqlClient()
	checkErr(err)

	defer mysqlClient.Close()

	s := &service{
		es:    esClient,
		mysql: mysqlClient,
	}

	fmt.Println("Querying for experts")

	for _, did := range deployments {
		did, _ := strconv.Atoi(did)
		fmt.Printf("Deployment: %d\n\n", did)

		lastID := 0
		var experts []*modelsMysql.Experts // needs to stay here. If we do below: `err,lastID,experts := s.fetchExperts(...)` it will override id all the time instead of reusing the declared one above

		// for {
		// getting the experts from the MySQL
		lastID, experts, err = s.mysql.FetchExperts(lastID, did, batchInsert)
		checkErr(err)

		if len(experts) == 0 {
			break
		}

		// indexing the experts onto ES
		err = s.es.IndexExperts(experts, batchInsert)
		checkErr(err)
		// }
	}
}

func checkErr(err error) {
	if err != nil {
		panic(err)
	}
}
