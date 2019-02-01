package main

/**
@todo aliases/nicknames
@todo replaced names
*/

import (
	"context"
	"database/sql"
	"encoding/csv"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"os"
	"strings"

	_ "github.com/go-sql-driver/mysql"
	"github.com/tomekwlod/okpii/db"
	_ "golang.org/x/net/html/charset"
	"golang.org/x/text/encoding/unicode"
	elastic "gopkg.in/olivere/elastic.v6"
)

const cdid = 2

type service struct {
	es    *elastic.Client
	mysql *sql.DB
	// logger  *log.Logger
}

func main() {

	esClient, err := db.Client()
	if err != nil {
		panic(err)
	}

	mysqlClient, err := db.MysqlClient()
	if err != nil {
		panic(err)
	}
	defer mysqlClient.Close()

	s := &service{
		es:    esClient,
		mysql: mysqlClient,
	}

	// Load a TXT file.
	f, err := os.Open("./file2.csv")
	if err != nil {
		panic(err)
	}
	defer f.Close()

	var i, count, all int
	for row := range processCSV(f) {
		i++

		// fmt.Println("[" + strconv.Itoa(i) + "] " + row[42] + " " + row[44])
		// 43-45

		match := s.findMatch(row[39], row[42], row[43], row[44])

		if match["id"] != nil {
			count++
		}
		all++

		// if i >= 1500 {
		// 	break
		// }
	}

	real := (float64(count) * 100) / float64(all)
	perc := (float64(count) * 100) / 2221

	fmt.Println("")
	fmt.Printf("All: %d\tMatched: %d\tPercent: %f\t Real: %f\n", all, count, perc, real)
}

func processCSV(rc io.Reader) (ch chan []string) {
	ch = make(chan []string, 10)

	go func() {
		dec := unicode.UTF8.NewDecoder()
		reader := dec.Reader(rc)

		r := csv.NewReader(reader)
		if _, err := r.Read(); err != nil { // read header
			log.Fatal(err)
		}
		defer close(ch)

		for {
			rec, err := r.Read()
			if err != nil {
				if err == io.EOF {
					break
				}
				log.Fatal(err)

			}

			// time.Sleep(2 * time.Second)

			ch <- rec
		}
	}()

	return
}

func (s *service) findMatch(id, fn, mn, ln string) map[string]interface{} {
	var row map[string]interface{}

	for _, i := range []int{1, 2} {
		q := querySelector(i, id, fn, mn, ln, cdid) //deployment=XX

		searchResult, err := s.es.Search().Index("experts").Type("data").Query(q).From(0).Size(10).Do(context.Background())
		if err != nil {
			panic(err)
		}

		// fmt.Printf("Query took %d milliseconds\n", searchResult.TookInMillis)

		// var data []map[string]interface{}
		if searchResult.Hits.TotalHits > 1 {
			fmt.Printf("  !!!!!!  Too many results %s \n", id)

			continue
		} else if searchResult.Hits.TotalHits == 1 {
			// fmt.Printf("  ==  Found a total of %d record(s) \n", searchResult.Hits.TotalHits)

			for _, hit := range searchResult.Hits.Hits {
				err := json.Unmarshal(*hit.Source, &row)

				if err != nil {
					panic(err)
				}

				// row["_type"] = hit.Type
				// row["_id"] = hit.Id
				fmt.Printf("{q%d} [%s] %s %s %s \t\t ====> \t [%s] %s, %s \tnpi:%d, ttid:%d\n", i, id, fn, mn, ln, hit.Id, row["name"], row["country"], row["npi"], row["ttid"])

				// 	data = append(data, row)
				return row
			}
		} else {
			// fmt.Println("  !==  No results found")
			continue
		}
	}

	return row
}

func querySelector(option int, id, fn, mn, ln string, did int) (q *elastic.BoolQuery) {
	switch option {
	case 1:
		return simpleQuery(id, fn, mn, ln, did)
	default:
		return aliasesQuery(id, fn, mn, ln, did)
	}
}

func simpleQuery(id, fn, mn, ln string, did int) (q *elastic.BoolQuery) {
	q = elastic.NewBoolQuery().Filter(elastic.NewMatchPhraseQuery("did", did))

	mnstr := " "
	if mn != "" {
		mnstr = " " + mn + " "
	}

	// bq := elastic.NewBoolQuery()

	// q.Must()
	q.Should(elastic.NewTermQuery("nameKeyword", fn+mnstr+ln))
	q.Should(elastic.NewTermQuery("nameKeyword", fn+" "+ln))
	q.Should(elastic.NewTermQuery("nameKeywordSquash", strings.Replace(fn+mnstr+ln, " ", "", -1)))
	q.Should(elastic.NewTermQuery("nameKeywordSquash", strings.Replace(fn+ln, " ", "", -1)))
	q.Should(elastic.NewTermQuery("nameKeywordRaw", strings.Replace(fn+mnstr+ln, " ", "", -1)))
	q.MinimumShouldMatch("1")

	return
}

func aliasesQuery(id, fn, mn, ln string, did int) (q *elastic.BoolQuery) {
	q = elastic.NewBoolQuery().Filter(elastic.NewMatchPhraseQuery("did", did))

	// should be:
	// name like "" --> OR we should have LN match and below
	// and
	// fn must one of the aliases

	// @todo: too risky -> mn needs to be included

	// mnstr := " "
	// if mn != "" {
	// 	mnstr = " " + mn + " "
	// }

	q.Must(elastic.NewMatchQuery("name", ln))
	q.Must(elastic.NewMatchPhraseQuery("aliases", fn))

	// mq := q.Must()
	// mq.Should(elastic.NewTermQuery("nameKeyword", fn+mnstr+ln))
	// mq.Should(elastic.NewTermQuery("nameKeyword", fn+" "+ln))
	// mq.Should(elastic.NewTermQuery("nameKeywordSquash", strings.Replace(fn+mnstr+ln, " ", "", -1)))
	// mq.Should(elastic.NewTermQuery("nameKeywordSquash", strings.Replace(fn+ln, " ", "", -1)))
	// mq.Should(elastic.NewTermQuery("nameKeywordRaw", strings.Replace(fn+mnstr+ln, " ", "", -1)))
	// q.MinimumShouldMatch("1")

	return
}
