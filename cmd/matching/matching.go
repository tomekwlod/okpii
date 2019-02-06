package main

/**

custName = row[41]  << sometimes not empty

Problem.1.
Below matching works per deployment. Since we have 22 deployments maybe it is better to ask only once and collect the matches for all the deployments.
But we have to also keep in mind that if we have a match through the simpleQuery we won't go any further and we may loose some not perfect matches. So either
to combine the queries together or call as many times as queries types (which also may be expensive process if we increase a number of the query types)

Problem.2.
OneKey has duplicates and it's imposible (without additional keys/locations/...) to distinguish the rows. Even with the locations from OneKey we normally
merge people with the same names anyway. So maybe there are two John's Smith's but not in our system. We should probably collect as many oneky's as
possible in additional mysql table and later decide either to unmerge experts or ignore the issue.

@todo aliases/nicknames
@todo replaced names
@todo introduce goroutines for eg. saving onekey to external db https://medium.com/@nikolay.bystritskiy/how-i-tried-to-do-things-asynchronously-in-golang-40e0c1a06a66
                also remember (In Go, when the main function exits, the program stops as well as all goroutines!!!!): https://medium.com/@matryer/very-basic-concurrency-for-beginners-in-go-663e63c6ba07 In Go, when the main function exits, the program stops.
*/

import (
	"context"
	"database/sql"
	"encoding/csv"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"strings"
	"time"

	_ "github.com/go-sql-driver/mysql"
	"github.com/mongodb/mongo-go-driver/bson"
	"github.com/mongodb/mongo-go-driver/mongo"
	"github.com/mongodb/mongo-go-driver/mongo/options"
	"github.com/tomekwlod/okpii/db"
	_ "golang.org/x/net/html/charset"
	"golang.org/x/text/encoding/unicode"
	elastic "gopkg.in/olivere/elastic.v6"
)

const cdid = 2
const germans = 2933
const all = 57251

type service struct {
	es    *elastic.Client
	mysql *sql.DB
	mongo *mongo.Database
	// logger  *log.Logger
}

func main() {
	t1 := time.Now()

	esClient, err := db.Client()
	if err != nil {
		panic(err)
	}

	mysqlClient, err := db.MysqlClient()
	if err != nil {
		panic(err)
	}
	defer mysqlClient.Close()

	mongoClient, err := db.MongoDB()
	if err != nil {
		panic(err)
	}

	s := &service{
		es:    esClient,
		mysql: mysqlClient,
		mongo: mongoClient,
	}

	ch := make(chan map[string]string) // one line from csv
	go s.onekeys(ch)

	var i, count, all int
	for m := range ch {
		i++

		fn, mn, ln := names(m)

		matches := s.findMatches(m["SRC_CUST_ID"], fn, mn, ln)

		for _, match := range matches {
			if match["id"] != nil {
				kid64 := match["id"].(float64)
				kid := int(kid64)

				if kid > 0 {
					s.update(kid, m["SRC_CUST_ID"])
					// st, _ := s.update(kid, row[39])
					// fmt.Println("----", st, "----")

					count++

				} else {
					fmt.Println("ID NOT VALID ", match["id"], kid)
				}
			}
		}

		all++
	}

	t2 := time.Now()

	real := (float64(count) * 100) / float64(all)
	perc := (float64(count) * 100) / germans

	fmt.Println("")
	fmt.Printf("All: %d\tMatched: %d\tPercent: %f\t Real: %f\n", all, count, perc, real)

	fmt.Printf("All done in: %v \n", t2.Sub(t1))
}

func (s *service) onekeys(out chan<- map[string]string) {
	defer close(out)

	// defining the collection
	collection := s.mongo.Collection("test")

	// filter := bson.D{{"SRC_CUST_ID", "WDEM01690729"}}
	filter := bson.D{{}}

	// Pass these options to the Find method
	options := options.Find()
	options.SetProjection(bson.D{
		{"_id", 0},
		{"SRC_FIRST_NAME", 1},
		{"SRC_MIDDLE_NAME", 1},
		{"SRC_LAST_NAME", 1},
		{"SRC_ORG_NAME", 1},
		{"SRC_CUST_ID", 1},
	})
	// options.SetLimit(10)

	cur, err := collection.Find(context.TODO(), filter, options)
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

func processCSV(rc io.Reader) (ch chan []string) {
	ch = make(chan []string, 10)

	go func() {
		dec := unicode.UTF8.NewDecoder()
		reader := dec.Reader(rc)

		r := csv.NewReader(reader)
		if _, err := r.Read(); err != nil { // read header
			panic(err)
		}
		defer close(ch)

		for {
			rec, err := r.Read()
			if err != nil {
				if err == io.EOF {
					break
				}
				panic(err)

			}

			// time.Sleep(2 * time.Second) // just to test the goroutines

			ch <- rec
		}
	}()

	return
}

func (s *service) update(id int, oneky string) (status int64, err error) {
	// @todo: also maybe ES needs to be updated to speed up re-runing the process

	result, err := s.mysql.Exec("UPDATE kol SET oneky=? WHERE id=?", oneky, id)
	if err != nil {
		return
	}

	status, err = result.RowsAffected()

	return
}

func (s *service) findMatches(id, fn, mn, ln string) (result []map[string]interface{}) {
	var row map[string]interface{}

	for _, i := range []int{1, 2} {

		// if i == 2 {
		// 	// testing q2 only!!
		// 	continue
		// }

		q := querySelector(i, id, fn, mn, ln, cdid) //deployment=XX

		searchResult, err := s.es.Search().Index("experts").Type("data").SearchSource(q).From(0).Size(10).Do(context.Background())
		if err != nil {
			panic(err)
		}

		// var data []map[string]interface{}
		if searchResult.Hits.TotalHits > 2 || (searchResult.Hits.TotalHits > 1 && i == 2) {
			fmt.Printf("  !!!!!!  Too many (%d) results %s; IMPLEMENT BOOST HERE AND TAKE THE FIRST MATCH!\n", searchResult.Hits.TotalHits, id)

			continue
		} else if searchResult.Hits.TotalHits == 1 || (searchResult.Hits.TotalHits == 2 && i == 1) {
			// fmt.Printf("  ==  Found a total of %d record(s) \n", searchResult.Hits.TotalHits)

			for _, hit := range searchResult.Hits.Hits {
				err := json.Unmarshal(*hit.Source, &row)

				if err != nil {
					panic(err)
				}

				result = append(result, row)

				fmt.Printf("{q%d} [%s] %s %s %s \t\t ====> \t [%s] %s, %s, npi: %v, ttid: %v\n", i, id, fn, mn, ln, hit.Id, row["name"], row["country"], row["npi"], row["ttid"])
			}

			return
		} else {
			// fmt.Printf("{q%d} [%s] %s %s %s \t\t ====> Not found\n", i, id, fn, mn, ln)

			continue
		}
	}

	return
}

func querySelector(option int, id, fn, mn, ln string, did int) (q *elastic.SearchSource) {
	switch option {
	case 1:
		return simpleQuery(id, fn, mn, ln, did)
	default:
		return aliasesQuery(id, fn, mn, ln, did)
	}
}

func simpleQuery(id, fn, mn, ln string, did int) (nss *elastic.SearchSource) {
	q := elastic.NewBoolQuery().Filter(
		elastic.NewMatchPhraseQuery("did", did),
	)

	mnstr := " "
	mn1 := ""
	if mn != "" {
		mnstr = " " + mn + " "
		mn1 = " " + FirstChar(mn) + " "
	}

	name := fn + mnstr + ln
	nameRaw := strings.Replace(strings.Replace(name, " ", "", -1), "-", "", -1)
	name1 := fn + mn1 + ln

	// John Mark Smith || Brian Surni <- with ASCII-folding
	q.Should(elastic.NewTermQuery("nameKeyword", name))
	// JohnMarkSmith || BrianSurni    <- with ASCII-folding
	q.Should(elastic.NewTermQuery("nameKeywordSquash", nameRaw))
	// JohnMarkSmith || BrianSurni    <- with ASCII-folding & lowercase
	q.Should(elastic.NewTermQuery("nameKeywordRaw", nameRaw))

	if name1 != name {
		// John M Smith <- with ASCII-folding
		q.Should(elastic.NewTermQuery("nameKeyword", name1))

		// JohnMSmith   <- with ASCII-folding
		nameRaw = strings.Replace(strings.Replace(name1, " ", "", -1), "-", "", -1)
		q.Should(elastic.NewTermQuery("nameKeywordSquash", nameRaw))
	}

	q.MinimumShouldMatch("1")

	nss = elastic.NewSearchSource().Query(q)

	return
}

func aliasesQuery(id, fn, mn, ln string, did int) (nss *elastic.SearchSource) {

	q := elastic.NewBoolQuery().
		Filter(
			elastic.NewMatchPhraseQuery("did", did),
			elastic.NewMatchPhraseQuery("ln", ln),
			MnSubQuery(mn),
		)

	q.Must(elastic.NewMatchPhraseQuery("aliases", fn))

	nss = elastic.NewSearchSource().Query(q)
	// PrintESQuery(nss)
	return
}

func MnSubQuery(mn string) (q *elastic.BoolQuery) {
	q = elastic.NewBoolQuery()

	if mn == "" {
		q.Must(elastic.NewMatchPhraseQuery("mn", ""))

		return
	}

	// Łukasz
	q.Should(elastic.NewMatchPhraseQuery("mn", mn))
	// Ł
	q.Should(elastic.NewMatchPhraseQuery("mn", FirstChar(mn)))
	// Ł.
	q.Should(elastic.NewMatchPhraseQuery("mn", FirstChar(mn)+"."))
	q.MinimumShouldMatch("1")

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

func names(m map[string]string) (fn, mn, ln string) {
	fn = m["SRC_FIRST_NAME"]
	mn = m["SRC_MIDDLE_NAME"]
	ln = m["SRC_LAST_NAME"]

	// if no MN but a space in FN then split
	if mn == "" {
		fne := strings.Split(fn, " ")

		if len(fne) > 1 {
			fn = fne[0]
			mn = strings.Join(fne[1:], " ")
		}
	}

	// if still nothing: -
	if mn == "" {
		fne := strings.Split(fn, "-")

		if len(fne) > 1 {
			fn = fne[0]
			mn = strings.Join(fne[1:], " ")
		}
	}

	return
}

func FirstChar(str string) (c string) {
	// value := "ü:ü to eo"
	// Convert string to rune slice before taking substrings.
	// ... This will handle Unicode characters correctly.
	//     Not needed for ASCII strings.
	runes := []rune(str)
	// fmt.Println("First 1:", string(runes[0]))
	// fmt.Println("Last 2:", string(runes[1:]))

	c = string(runes[0])

	return
}

// remove me later
// curl -X GET "localhost:9202/experts/_search" -H 'Content-Type: application/json' -d'
// {
// "query": {
//     "bool": {
// 		"filter": {
// 		  "match_phrase": {
// 			"did": {
// 			  "query": 2
// 			}
// 		  }
// 		},
// 		"minimum_should_match": "1",
// 		"should": [
// 		  {
// 			"term": {
// 			  "nameKeyword": "Jens Malte Baron"
// 			}
// 		  },
// 		  {
// 			"term": {
// 			  "nameKeywordSquash": "JensMalteBaron"
// 			}
// 		  },
// 		  {
// 			"term": {
// 			  "nameKeywordRaw": "JensMalteBaron"
// 			}
// 		  },
// 		  {
// 			"term": {
// 			  "nameKeyword": "Jens M Baron"
// 			}
// 		  },
// 		  {
// 			"term": {
// 			  "nameKeywordSquash": "JensMBaron"
// 			}
// 		  }
// 		]
// 	  }
// 	}
// }
// ' | json_pp
