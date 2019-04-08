package main

/*
DUMP
Dumps data from MySQL to Elasticsearch
*/

import (
	"flag"
	"fmt"
	"strconv"

	modelsES "github.com/tomekwlod/okpii/models/es"
	modelsMysql "github.com/tomekwlod/okpii/models/mysql"
	"github.com/tomekwlod/okpii/tools"

	_ "github.com/go-sql-driver/mysql"
	_ "golang.org/x/net/html/charset"
)

const batchInsert = 3000

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
	didFlag := flag.String(
		"did",
		"1,2,3,9,10,11,12,13,14,15,16,17,22,24,25,26,27,28,29,30,31,32",
		"A deployments list comma separated od a single deployment")
	countriesFlag := flag.String(
		"countries",
		"",
		"Comma separated list of countries, default: all the countries")

	// once done with the flags/arguments let's parse them
	flag.Parse()

	deployments, err := tools.Deployments(*didFlag)
	if err != nil {
		panic(err)
	}
	fmt.Printf("\n> Starting with: %v deployment(s)\n", deployments)

	countries, err := tools.Countries(*countriesFlag)
	if err != nil {
		panic(err)
	}
	fmt.Printf("\n> Countries: %v\n", countries)

	esClient, err := modelsES.ESClient()
	checkErr(err)

	mysqlClient, err := modelsMysql.MysqlClient()
	checkErr(err)

	defer mysqlClient.Close()

	s := &service{
		es:    esClient,
		mysql: mysqlClient,
	}

	fmt.Println("Removing old data")
	deleted, err := s.es.RemoveData()
	if err != nil {
		panic(err)
	}
	fmt.Printf("Removed %d rows\n\n", deleted)

	fmt.Println("Querying for experts")

	for _, did := range deployments {
		did, _ := strconv.Atoi(did)
		fmt.Printf("\nDeployment: %d\n\n", did)

		var experts []*modelsMysql.Experts // needs to stay here. If we do below: `err,lastID,experts := s.fetchExperts(...)` it will override id all the time instead of reusing the declared one above

		lastID := 0
		for {
			// getting the experts from the MySQL
			lastID, experts, err = s.mysql.FetchExperts(lastID, did, batchInsert, countries)
			checkErr(err)

			quantity := len(experts)

			// stop if no results
			if quantity == 0 {
				break
			}

			// indexing the experts onto ES
			err = s.es.IndexExperts(experts, batchInsert)
			checkErr(err)

		}
	}
}

func checkErr(err error) {
	if err != nil {
		panic(err)
	}
}
