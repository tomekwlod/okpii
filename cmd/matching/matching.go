package main

/**

Problem.1.
Below matching works per deployment. Since we have 22 deployments maybe it is better to ask only once and collect the matches for all the deployments.
But we have to also keep in mind that if we have a match through the simpleQuery we won't go any further and we may loose some not perfect matches. So either
to combine the queries together or call as many times as queries types (which also may be expensive process if we increase a number of the query types)

Problem.2.
OneKey has duplicates and it's imposible (without additional keys/locations/...) to distinguish the rows. Even with the locations from OneKey we normally
merge people with the same names anyway. So maybe there are two John's Smith's but not in our system. We should probably collect as many oneky's as
possible in additional mysql table and later decide either to unmerge experts or ignore the issue.



@todo add aliases/nicknames to every search step!
@todo replaced names
@todo introduce goroutines for eg. saving onekey to external db https://medium.com/@nikolay.bystritskiy/how-i-tried-to-do-things-asynchronously-in-golang-40e0c1a06a66
                also remember (In Go, when the main function exits, the program stops as well as all goroutines!!!!): https://medium.com/@matryer/very-basic-concurrency-for-beginners-in-go-663e63c6ba07 In Go, when the main function exits, the program stops.
*/

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"log"
	"strconv"
	"strings"
	"sync"
	"time"

	_ "github.com/go-sql-driver/mysql"
	"github.com/mongodb/mongo-go-driver/bson"
	"github.com/mongodb/mongo-go-driver/bson/primitive"
	"github.com/mongodb/mongo-go-driver/mongo"
	"github.com/mongodb/mongo-go-driver/mongo/options"
	"github.com/tomekwlod/okpii/db"
	_ "golang.org/x/net/html/charset"
	elastic "gopkg.in/olivere/elastic.v6"
)

const cdid = "15,16,17,22,24,25,26,27,28,29,30,31,32"

type service struct {
	es    *elastic.Client
	mysql *sql.DB
	mongo *mongo.Database
	// logger  *log.Logger
}

func main() {
	t1 := time.Now()
	var wg sync.WaitGroup

	esClient, err := db.ESClient()
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

	// Getting the experts from MongoDB line-by-line
	ch := make(chan map[string]string) // one line only
	go s.onekeys(ch)

	var i int
	for m := range ch {
		i++

		fn, mn, ln := names(m)

		for _, did := range strings.Split(cdid, ",") {
			did, _ := strconv.Atoi(did)

			queryNumber, matches := s.findMatches(did, m["SRC_CUST_ID"], m["City"], fn, mn, ln)
			// _, matches := s.findMatches(did, m["SRC_CUST_ID"], m["City"], fn, mn, ln)

			for _, match := range matches {
				if match["id"] != nil {
					kid64 := match["id"].(float64)
					kid := int(kid64)

					// if queryNumber != 4 {
					// 	continue
					// }

					if kid > 0 {
						fmt.Printf("{q%d}: [%s] %s %s %s {%s}\t\t ====> \t [%d] %s, {%s} npi: %v, ttid: %v\n",
							queryNumber, m["SRC_CUST_ID"], fn, mn, ln, m["City"],
							kid, match["name"], match["city"], match["npi"], match["ttid"],
						)

						wg.Add(1)
						go s.update(&wg, kid, did, m["SRC_CUST_ID"])

					} else {
						fmt.Println("ID NOT VALID ", match["id"], kid)
					}
				}
			}
		}

	}

	t2 := time.Now()

	wg.Wait()
	fmt.Printf("\nAll done in: %v \n", t2.Sub(t1))
}

func (s *service) isInOneKeyDB(fn, mn, ln string) bool {
	// defining the collection
	collection := s.mongo.Collection("test")

	// filter := bson.D{{"SRC_CUST_ID", "WDEM01690729"}}
	filter := bson.M{
		"SRC_FIRST_NAME":  fn,
		"SRC_MIDDLE_NAME": mn,
		"SRC_LAST_NAME":   ln,
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
func (s *service) countOneKeyOcc(fn, ln string) int64 {
	// defining the collection
	collection := s.mongo.Collection("test")

	// filter := bson.D{{"SRC_CUST_ID", "WDEM01690729"}}
	filter := bson.M{
		"SRC_FIRST_NAME": primitive.Regex{Pattern: "^" + fn + ".*", Options: ""},
		"SRC_LAST_NAME":  ln,
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
		{"City", 1},
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

func (s *service) update(wg *sync.WaitGroup, id, did int, oneky string) (status int64, err error) {
	defer wg.Done()
	// @todo: also maybe ES needs to be updated to speed up re-runing the process
	// @todo: do it in async mode!

	// result, err := s.mysql.Exec("UPDATE kol SET oneky=? WHERE id=?", oneky, id)
	result, err := s.mysql.Exec("INSERT INTO kol__onekey SET onekey=?, kid=?, did=?", oneky, id, did)
	if err != nil {
		return
	}

	status, err = result.RowsAffected()

	return
}

func (s *service) findMatches(did int, id, city, fn, mn, ln string) (queryNumber int, result []map[string]interface{}) {
	if strings.Replace(fn, " ", "", -1) == "" {
		// if no FN we should just continue; if causes too much hassle
		return
	}

	for _, queryNumber = range []int{1, 2, 3, 4} {

		result = s.search(queryNumber, id, fn, mn, ln, city, did) //deployment=XX

		if len(result) == 0 {
			// fmt.Printf("{q%d} [%s] %s %s %s \t\t ====> Not results found\n", i, id, fn, mn, ln)

			continue
		}

		return
	}

	return
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
	if len(str) == 0 {
		return ""
	}
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

func (s *service) search(option int, id, fn, mn, ln, city string, did int) (result []map[string]interface{}) {
	switch option {
	case 1:
		return s.simple(id, fn, mn, ln, city, did)
	case 2:
		return s.aliases(id, fn, mn, ln, city, did)
	case 3:
		return s.short(id, fn, mn, ln, city, did)
	case 4:
		return s.noMiddleNameOnly(id, fn, mn, ln, city, did)
	// case 4:
	// 	// this is quite risky. there should be a check if only one available match!
	// 	return s.noMiddleName(id, fn, mn, ln, city, did)
	// // case 5:
	// // 	return s.madnessTemp(id, fn, mn, ln, city, did)
	// case 5:
	// 	return s.madness2Temp(id, fn, mn, ln, city, did)
	default:
		return s.testSearch(id, fn, mn, ln, city, did)
		return nil
	}
}

func (s *service) simple(id, fn, mn, ln, city string, did int) (result []map[string]interface{}) {
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

	// this is the best if we want to print the query for the test purposes
	nss := elastic.NewSearchSource().Query(q)

	searchResult, err := s.es.Search().Index("experts").Type("data").SearchSource(nss).From(0).Size(10).Do(context.Background())
	if err != nil {
		panic(err)
	}

	if searchResult.Hits.TotalHits == 0 {
		// fmt.Printf("[%s] %s %s %s \t\t ====> Not found\n", id, fn, mn, ln)
		return nil
	}

	if searchResult.Hits.TotalHits > 2 {
		// fmt.Printf("  !!!!!! (simple)  Too many (%d) results %s;\n", searchResult.Hits.TotalHits, id)
		// fmt.Printf("[%s] %s %s %s\n", id, fn, mn, ln)

		for _, hit := range searchResult.Hits.Hits {
			var row map[string]interface{}

			err := json.Unmarshal(*hit.Source, &row)
			if err != nil {
				panic(err)
			}
			// fmt.Printf(" > [%s] %s %s %s\n", hit.Id, row["fn"], row["mn"], row["ln"])
		}
		return nil
	}

	for _, hit := range searchResult.Hits.Hits {
		var row map[string]interface{}

		err := json.Unmarshal(*hit.Source, &row)
		if err != nil {
			panic(err)
		}

		result = append(result, row)

		// fmt.Printf("[%s] %s %s %s {%s}\t\t ====> \t [%s] %s, {%s} npi: %v, ttid: %v\n", id, fn, mn, ln, city, hit.Id, row["name"], row["city"], row["npi"], row["ttid"])
	}

	return
}

func (s *service) aliases(id, fn, mn, ln, city string, did int) (result []map[string]interface{}) {
	q := elastic.NewBoolQuery().
		Filter(
			elastic.NewMatchPhraseQuery("did", did),
			elastic.NewMatchPhraseQuery("ln", ln),
			MnSubQuery(mn),
		)

	q.Must(elastic.NewMatchPhraseQuery("aliases", fn))

	nss := elastic.NewSearchSource().Query(q)

	searchResult, err := s.es.Search().Index("experts").Type("data").SearchSource(nss).From(0).Size(10).Do(context.Background())
	if err != nil {
		panic(err)
	}

	if searchResult.Hits.TotalHits == 0 {
		// fmt.Printf("[%s] %s %s %s \t\t ====> Not found\n", id, fn, mn, ln)
		return nil
	}

	if searchResult.Hits.TotalHits > 2 {
		// fmt.Printf("  !!!!!! (aliases) Too many (%d) results %s;\n", searchResult.Hits.TotalHits, id)
		// fmt.Printf("[%s] %s %s %s\n", id, fn, mn, ln)

		for _, hit := range searchResult.Hits.Hits {
			var row map[string]interface{}

			err := json.Unmarshal(*hit.Source, &row)
			if err != nil {
				panic(err)
			}
			// fmt.Printf(" > [%s] %s %s %s\n", hit.Id, row["fn"], row["mn"], row["ln"])
		}
		return nil
	}

	for _, hit := range searchResult.Hits.Hits {
		var row map[string]interface{}

		err := json.Unmarshal(*hit.Source, &row)
		if err != nil {
			panic(err)
		}

		result = append(result, row)

		// fmt.Printf("[%s] %s %s %s {%s}\t\t ====> \t [%s] %s, {%s} npi: %v, ttid: %v\n", id, fn, mn, ln, city, hit.Id, row["name"], row["city"], row["npi"], row["ttid"])
	}

	return
}

func (s *service) short(id, fn, mn, ln, city string, did int) (result []map[string]interface{}) {
	q := elastic.NewBoolQuery().Filter(
		elastic.NewMatchPhraseQuery("did", did),
		elastic.NewMatchPhraseQuery("ln", ln),
	)

	if mn == "" {
		// this case is only for the names with MN included
		return nil
	}

	fn = strings.Replace(fn, ".", "", -1)
	mn = strings.Replace(mn, ".", "", -1)

	fnl := len(fn)
	mnl := len(mn)

	if fnl <= 1 && mnl <= 1 {
		// nothing to do if short names already; squash would do the job
		return nil
	}

	mn1q := elastic.NewBoolQuery()
	mn1q.Should(elastic.NewMatchPhraseQuery("mn", mn))
	if mnl > 1 {
		mn1q.Should(elastic.NewMatchPhraseQuery("mn", FirstChar(mn)))
	}
	mn1q.MinimumShouldMatch("1")

	fn1q := elastic.NewBoolQuery()
	fn1q.Should(elastic.NewMatchPhraseQuery("fn", fn))
	if fnl > 1 {
		fn1q.Should(elastic.NewMatchPhraseQuery("fn", FirstChar(fn)))
	}
	fn1q.MinimumShouldMatch("1")

	q.Must(mn1q, fn1q)

	nss := elastic.NewSearchSource().Query(q)

	searchResult, err := s.es.Search().Index("experts").Type("data").SearchSource(nss).From(0).Size(10).Do(context.Background())
	if err != nil {
		panic(err)
	}

	if searchResult.Hits.TotalHits == 0 {
		// fmt.Printf("[%s] %s %s %s \t\t ====> Not found\n", id, fn, mn, ln)
		return nil
	}

	if searchResult.Hits.TotalHits > 1 {
		// fmt.Printf("  !!!!!! (short) Too many (%d) results %s;\n", searchResult.Hits.TotalHits, id)
		// fmt.Printf("[%s] %s %s %s\n", id, fn, mn, ln)

		for _, hit := range searchResult.Hits.Hits {
			var row map[string]interface{}

			err := json.Unmarshal(*hit.Source, &row)
			if err != nil {
				panic(err)
			}
			// fmt.Printf(" > [%s] %s %s %s\n", hit.Id, row["fn"], row["mn"], row["ln"])
		}
		return nil
	}

	for _, hit := range searchResult.Hits.Hits {
		var row map[string]interface{}

		err := json.Unmarshal(*hit.Source, &row)
		if err != nil {
			panic(err)
		}

		result = append(result, row)

		// fmt.Printf("[%s] %s %s %s {%s}\t\t ====> \t [%s] %s, {%s} npi: %v, ttid: %v\n", id, fn, mn, ln, city, hit.Id, row["name"], row["city"], row["npi"], row["ttid"])
	}

	return
}

func (s *service) noMiddleName(id, fn, mn, ln, city string, did int) (result []map[string]interface{}) {
	// {q4}: [WDEM00121088] Jana Marie Worm {ESSEN}		 ====> 	 [5597205] J Worm, {København} npi: 0, ttid: 0
	return nil

	q := elastic.NewBoolQuery().Filter(
		elastic.NewMatchPhraseQuery("did", did),
		elastic.NewMatchPhraseQuery("ln", ln),
		elastic.NewTermQuery("mn", ""),
	)

	if mn == "" {
		// this case is only for the names with MN included
		return nil
	}

	fn = strings.Replace(fn, ".", "", -1)
	mn = strings.Replace(mn, ".", "", -1)

	fnl := len(fn)
	mnl := len(mn)

	if fnl <= 1 && mnl <= 1 {
		// nothing to do if short names already; squash would do the job
		return nil
	}

	fn1q := elastic.NewBoolQuery()
	fn1q.Should(elastic.NewMatchPhraseQuery("fn", fn))
	if fnl > 1 {
		fn1q.Should(elastic.NewMatchPhraseQuery("fn", FirstChar(fn)))
	}
	fn1q.MinimumShouldMatch("1")

	q.Must(fn1q)

	nss := elastic.NewSearchSource().Query(q)

	searchResult, err := s.es.Search().Index("experts").Type("data").SearchSource(nss).From(0).Size(10).Do(context.Background())
	if err != nil {
		panic(err)
	}

	if searchResult.Hits.TotalHits == 0 {
		// fmt.Printf("[%s] %s %s %s \t\t ====> Not found\n", id, fn, mn, ln)
		return nil
	}

	// checking if there is someone with the above firstnames but with different middlenames!!
	if s.mnConflict(fn, mn, ln, did) {
		// fmt.Printf(" >>>>> [%s] %s %s %s \t Middle names conflict detected\n", id, fn, mn, ln)

		return nil
	}

	for _, hit := range searchResult.Hits.Hits {
		var row map[string]interface{}

		err := json.Unmarshal(*hit.Source, &row)
		if err != nil {
			panic(err)
		}

		result = append(result, row)

		// fmt.Printf("[%s] %s %s %s {%s}\t\t ====> \t [%s] %s, {%s} npi: %v, ttid: %v\n", id, fn, mn, ln, city, hit.Id, row["name"], row["city"], row["npi"], row["ttid"])
	}

	return
}

func (s *service) noMiddleNameOnly(id, fn, mn, ln, city string, did int) (result []map[string]interface{}) {
	q := elastic.NewBoolQuery().Filter(
		elastic.NewMatchPhraseQuery("did", did),
		elastic.NewMatchPhraseQuery("ln", ln),
		elastic.NewTermQuery("mn", ""),
	)

	if mn != "" {
		// this case is only for the names with NO MN included

		// ---------------------------
		// [WDEM02118277]
		// ->Ralf  Dittrich {OSNABRÜCK}             ====> Found [did:1]: 5711743
		// 0.   R  Dittrich {}
		//
		// We want to match this only if R* Dittrich exist in OneKey Db only once!
		return nil
	}

	q.Must(elastic.NewMatchPhraseQuery("fn", FirstChar(fn)))

	nss := elastic.NewSearchSource().Query(q)

	searchResult, err := s.es.Search().Index("experts").Type("data").SearchSource(nss).From(0).Size(10).Do(context.Background())
	if err != nil {
		panic(err)
	}

	if searchResult.Hits.TotalHits == 0 {
		// fmt.Printf("[%s] %s %s %s \t\t ====> Not found\n", id, fn, mn, ln)
		return nil
	}

	if searchResult.Hits.TotalHits > 1 {
		fmt.Printf("[%s] \t\t ====> Too many results! Should be only one like: %s %s\n", id, FirstChar(fn), ln)
		return nil
	}

	for _, hit := range searchResult.Hits.Hits {
		total := s.countOneKeyOcc(FirstChar(fn), ln)

		if total == 1 {
			var row map[string]interface{}

			err := json.Unmarshal(*hit.Source, &row)
			if err != nil {
				panic(err)
			}

			result = append(result, row)

			// fmt.Printf("[%s] %s %s %s {%s}\t\t ====> \t [%s] %s, {%s} npi: %v, ttid: %v\n", id, fn, mn, ln, city, hit.Id, row["name"], row["city"], row["npi"], row["ttid"])
		}
	}

	return
}

// func (s *service) madnessTemp(id, fn, mn, ln, city string, did int) (result []map[string]interface{}) {
// 	q := elastic.NewBoolQuery().Filter(
// 		elastic.NewMatchPhraseQuery("did", did),
// 		elastic.NewMatchPhraseQuery("ln", ln),
// 		// madness for now is only for NO MIDDLE NAMES pairs
// 		elastic.NewTermQuery("mn", ""),
// 	)

// 	if mn != "" {
// 		// this case is only for the names WITHOUT MN included
// 		return nil
// 	}

// 	fn = strings.Replace(fn, ".", "", -1)

// 	// only fn1
// 	q.Must(elastic.NewMatchPhraseQuery("fn", FirstChar(fn)))

// 	nss := elastic.NewSearchSource().Query(q)

// 	searchResult, err := s.es.Search().Index("experts").Type("data").SearchSource(nss).From(0).Size(10).Do(context.Background())
// 	if err != nil {
// 		panic(err)
// 	}

// 	if searchResult.Hits.TotalHits == 0 {
// 		// fmt.Printf("[%s] %s %s %s \t\t ====> Not found\n", id, fn, mn, ln)
// 		return nil
// 	}

// 	if searchResult.Hits.TotalHits > 1 {
// 		// fmt.Printf("[%s] %s %s %s \t\t ====> Too many madness results!\n", id, fn, mn, ln)
// 		return nil
// 	}

// 	// checking if there is someone with the above fn1* + mn* + ln
// 	// TEMPORARY disabled for the presentation purposes
// 	// if s.madnessConflict(fn, mn, ln, cdid) {
// 	// 	fmt.Printf(" >>>>> [%s] %s %s %s \t Madness conflict detected\n", id, fn, mn, ln)
// 	// 	return nil
// 	// }

// 	for _, hit := range searchResult.Hits.Hits {
// 		var row map[string]interface{}

// 		err := json.Unmarshal(*hit.Source, &row)
// 		if err != nil {
// 			panic(err)
// 		}

// 		result = append(result, row)

// 		fmt.Printf("[%s] %s %s %s {%s}\t\t ====> \t [%s] %s, {%s} npi: %v, ttid: %v\n", id, fn, mn, ln, city, hit.Id, row["name"], row["city"], row["npi"], row["ttid"])
// 	}

// 	return
// }

func (s *service) madness2Temp(id, fn, mn, ln, city string, did int) (result []map[string]interface{}) {
	// {q5}: [WDEM00089847] Thomas  Braun {MÜNCHEN}		 ====> 	 [2549379] Thomas M Braun, {Ann Arbor} npi: 0, ttid: 0
	// {q5}: [WDEM00089847] Thomas  Braun {MÜNCHEN}		 ====> 	 [5651374] Thomas J Braun, {Denver} npi: 1.841219474e+09, ttid: 120569
	return nil

	q := elastic.NewBoolQuery().Filter(
		elastic.NewMatchPhraseQuery("did", did),
		elastic.NewMatchPhraseQuery("ln", ln),
	)

	if mn != "" {
		// this case is only for the names WITHOUT MN included
		return nil
	}

	fn = strings.Replace(fn, ".", "", -1)

	if len(fn) == 1 {
		return nil
	}

	// only fn1
	q.Must(elastic.NewMatchPhraseQuery("fn", fn))
	q.MustNot(elastic.NewTermQuery("mn", ""))

	nss := elastic.NewSearchSource().Query(q)

	searchResult, err := s.es.Search().Index("experts").Type("data").SearchSource(nss).From(0).Size(10).Do(context.Background())
	if err != nil {
		panic(err)
	}

	if searchResult.Hits.TotalHits == 0 {
		// fmt.Printf("[%s] %s %s %s \t\t ====> Not found\n", id, fn, mn, ln)
		return nil
	}

	// if searchResult.Hits.TotalHits > 1 {
	// 	// fmt.Printf("[%s] %s %s %s \t\t ====> Too many madness results!\n", id, fn, mn, ln)
	// 	return nil
	// }

	for _, hit := range searchResult.Hits.Hits {
		var row map[string]interface{}

		err := json.Unmarshal(*hit.Source, &row)
		if err != nil {
			panic(err)
		}

		// checking if this exact mach already exists in the OneKey DB; if so: skip, otherwise: ok to go
		if s.isInOneKeyDB(fn, mn, ln) {
			fmt.Println(" >><< Better match found and will be used later >><<")

			continue
		}

		result = append(result, row)

		// fmt.Printf("[%s] %s %s %s {%s}\t\t ====> \t [%s] %s, {%s} npi: %v, ttid: %v\n", id, fn, mn, ln, city, hit.Id, row["name"], row["city"], row["npi"], row["ttid"])
	}

	return
}

func (s *service) testSearch(id, fn, mn, ln, city string, did int) (result []map[string]interface{}) {
	q := elastic.NewBoolQuery().Filter(
		elastic.NewMatchPhraseQuery("did", did),
		elastic.NewMatchPhraseQuery("ln", ln),
		elastic.NewPrefixQuery("fn", FirstChar(fn)),
	)

	nss := elastic.NewSearchSource().Query(q)

	searchResult, err := s.es.Search().Index("experts").Type("data").SearchSource(nss).From(0).Size(200).Do(context.Background())
	if err != nil {
		panic(err)
	}

	if searchResult.Hits.TotalHits > 0 {
		n := []string{}
		ids := []string{}

		for i, hit := range searchResult.Hits.Hits {
			var row map[string]interface{}

			err := json.Unmarshal(*hit.Source, &row)
			if err != nil {
				panic(err)
			}

			n = append(n, strconv.Itoa(i)+". "+row["fn"].(string)+" "+row["mn"].(string)+" "+row["ln"].(string)+" {"+row["city"].(string)+"}")

			kid64 := row["id"].(float64)
			kid := int(kid64)
			ids = append(ids, strconv.Itoa(kid))
		}

		fmt.Printf("\n\n---------------------------\n[%s]\n->%s %s %s {%s} \t\t ====> Found [did:%d]: %s\n%s \n\n", id, fn, mn, ln, city, did, strings.Join(ids, ","), strings.Join(n, "\n"))
	} else {
		// fmt.Printf("{q%d} [%s] %s %s %s \t\t ====> Not found\n", i, id, fn, mn, ln)
	}

	return
}

func (s *service) mnConflict(fn, mn, ln string, did int) bool {
	q := elastic.NewBoolQuery().Filter(
		elastic.NewMatchPhraseQuery("did", did),
		elastic.NewMatchPhraseQuery("ln", ln),
	)

	fn = strings.Replace(fn, ".", "", -1)
	mn = strings.Replace(mn, ".", "", -1)

	fnl := len(fn)
	mnl := len(mn)

	fn1q := elastic.NewBoolQuery()
	fn1q.Should(elastic.NewMatchPhraseQuery("fn", fn))
	if fnl > 1 {
		fn1q.Should(elastic.NewMatchPhraseQuery("fn", FirstChar(fn)))
	}
	fn1q.MinimumShouldMatch("1")

	q.Must(fn1q)
	q.MustNot(elastic.NewTermQuery("mn", ""))

	nss := elastic.NewSearchSource().Query(q)

	searchResult, err := s.es.Search().Index("experts").Type("data").SearchSource(nss).From(0).Size(10).Do(context.Background())
	if err != nil {
		panic(err)
	}

	if searchResult.Hits.TotalHits == 0 {
		// no results, no conflicts
		return false
	}

	for _, hit := range searchResult.Hits.Hits {
		var row map[string]interface{}

		err := json.Unmarshal(*hit.Source, &row)
		if err != nil {
			panic(err)
		}

		rmn := row["mn"].(string)
		rmnl := len(rmn)

		if mnl == 1 && rmnl == 1 {
			if mn != row["mn"] {
				// mn1 != rowMn1 => conflict
				return true
			}
		} else if mnl == 1 && FirstChar(rmn) != mn {
			return true
		} else if rmnl == 1 && FirstChar(mn) != rmn {
			return true
		} else if mnl > 1 && rmnl > 1 && mn != rmn {
			return true
		}

		// fmt.Printf("[%s] %s %s %s {%s}\t\t ====> \t [%s] %s, {%s} npi: %v, ttid: %v\n", id, fn, mn, ln, city, hit.Id, row["name"], row["city"], row["npi"], row["ttid"])
	}

	return false
}
func (s *service) madnessConflict(fn, mn, ln string, did int) bool {
	q := elastic.NewBoolQuery().Filter(
		elastic.NewMatchPhraseQuery("did", did),
		elastic.NewMatchPhraseQuery("ln", ln),
		// elastic.NewTermQuery("mn", ""),
	)

	q.Must(elastic.NewPrefixQuery("fn", FirstChar(fn)))

	nss := elastic.NewSearchSource().Query(q)

	searchResult, err := s.es.Search().Index("experts").Type("data").SearchSource(nss).From(0).Size(10).Do(context.Background())
	if err != nil {
		panic(err)
	}

	if searchResult.Hits.TotalHits == 0 {
		// no results, no conflicts
		return false
	}

	if searchResult.Hits.TotalHits > 1 {
		// no results, no conflicts
		return true
	}

	// no conflicts
	return false
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

// export me
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
