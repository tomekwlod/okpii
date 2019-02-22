package main

/*
DUMP
Dumps data from MySQL to Elasticsearch
*/

import (
	"fmt"
	"strconv"
	"strings"

	modelsES "github.com/tomekwlod/okpii/models/es"
	modelsMysql "github.com/tomekwlod/okpii/models/mysql"

	_ "github.com/go-sql-driver/mysql"
	_ "golang.org/x/net/html/charset"
)

const cdid = "9"
const batchInsert = 1000

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

	for _, did := range strings.Split(cdid, ",") {
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
		err = s.es.IndexExperts(experts)
		checkErr(err)
		// }
	}
}

// func transform(rows *sql.Rows) (e *Experts, err error) {
// 	var id, did int // if nullable then if should be sql.NullInt64
// 	var npi, ttid, position sql.NullInt64
// 	var fn, mn, ln, city, country sql.NullString // not just string here because of nulls
// 	var fn1, fn2, fn3, fn4 sql.NullString

// 	err = rows.Scan(&id, &fn, &ln, &mn, &npi, &ttid, &did, &position, &city, &country, &fn1, &fn2, &fn3, &fn4)
// 	if err != nil {
// 		return
// 	}

// 	aliases := mergeAliases(fn1, fn2, fn3, fn4)

// 	mnstr := " "
// 	if mn.String != "" {
// 		mnstr = " " + mn.String + " "
// 	}
// 	name := "" + fn.String + mnstr + ln.String
// 	squash := strings.Replace(strings.Replace(name, "-", "", -1), " ", "", -1)

// 	e = &Experts{
// 		ID:                id,
// 		Did:               did,
// 		NPI:               int(npi.Int64),
// 		TTID:              int(ttid.Int64),
// 		Fn:                fn.String,
// 		Mn:                mn.String,
// 		Ln:                ln.String,
// 		Name:              name,
// 		NameKeyword:       name,
// 		NameKeywordSquash: squash,
// 		NameKeywordRaw:    squash,
// 		Deleted:           0,
// 		FNDash:            strings.Contains(fn.String, "-"),
// 		FNDot:             strings.Contains(fn.String, "."),
// 		Position:          int(position.Int64),
// 		City:              city.String,
// 		Country:           country.String,
// 		Aliases:           aliases,
// 	}

// 	return
// }

func checkErr(err error) {
	if err != nil {
		panic(err)
	}
}
