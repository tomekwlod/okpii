package models

import (
	"context"
	"encoding/json"
	"fmt"
	"strconv"
	"strings"

	modelsMysql "github.com/tomekwlod/okpii/models/mysql"
	strutils "github.com/tomekwlod/utils/strings"
	elastic "gopkg.in/olivere/elastic.v6"
)

func (db *DB) SimpleSearch(id, custName, fn, mn, ln, city string, did int, exclIDs []string) []map[string]interface{} {
	result := []map[string]interface{}{}

	q := elastic.NewBoolQuery().Filter(
		elastic.NewMatchPhraseQuery("did", did),
	)

	if len(exclIDs) > 0 {
		for _, ID := range exclIDs {
			q.MustNot(elastic.NewMatchPhraseQuery("id", ID))
		}
	}

	mnstr := " "
	mn1 := ""
	if mn != "" {
		mnstr = " " + mn + " "
		mn1 = " " + strutils.FirstChar(mn) + " "
	}

	name := fn + mnstr + ln
	nameRaw := strings.Replace(strings.Replace(name, " ", "", -1), "-", "", -1)
	name1 := fn + mn1 + ln

	// John Mark Smith || Brian Surni <- with ASCII-folding
	q.Should(elastic.NewTermQuery("nameKeyword", name))
	q.Should(elastic.NewTermQuery("nameKeyword.german", name))
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

	searchResult, err := db.Search().Index("experts").Type("data").SearchSource(nss).From(0).Size(10).Do(context.Background())
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

	return result
}

func (db *DB) ShortSearch(id, custName, fn, mn, ln, city string, did int, exclIDs []string) (result []map[string]interface{}) {
	q := elastic.NewBoolQuery().
		Filter(
			elastic.NewMatchPhraseQuery("did", did),
		).
		Should(
			elastic.NewMatchPhraseQuery("ln", ln),
			elastic.NewMatchPhraseQuery("ln.german", ln),
		).MinimumShouldMatch("1")

	if mn == "" {
		// this case is only for the names with MN included
		return nil
	}

	if len(exclIDs) > 0 {
		for _, ID := range exclIDs {
			q.MustNot(elastic.NewMatchPhraseQuery("id", ID))
		}
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
		mn1q.Should(elastic.NewMatchPhraseQuery("mn", strutils.FirstChar(mn)))
	} else if mnl == 1 {
		// this is fine, may be a bit risky here:
		// First M Last  <-- input
		// First Middle Last
		// First Maybe  Last
		// both would match, so either we check against the Mongodb or for now ignore it (maybe also location check?)
		mn1q.Should(elastic.NewMatchPhrasePrefixQuery("mn", mn))
	}
	mn1q.MinimumShouldMatch("1")

	fn1q := elastic.NewBoolQuery()
	fn1q.Should(elastic.NewMatchPhraseQuery("fn", fn))
	fn1q.Should(elastic.NewMatchPhraseQuery("aliases", fn))
	if fnl > 1 {
		fn1q.Should(elastic.NewMatchPhraseQuery("fn", strutils.FirstChar(fn)))
	}
	fn1q.MinimumShouldMatch("1")

	q.Must(mn1q, fn1q)

	nss := elastic.NewSearchSource().Query(q)

	searchResult, err := db.Search().Index("experts").Type("data").SearchSource(nss).From(0).Size(10).Do(context.Background())
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

func (db *DB) NoMiddleNameSearch(id, custName, fn, mn, ln, city string, did int, exclIDs []string) (result []map[string]interface{}) {
	q := elastic.NewBoolQuery().Filter(
		elastic.NewMatchPhraseQuery("did", did),
		elastic.NewTermQuery("mn", ""),
	).Should(
		elastic.NewMatchPhraseQuery("ln", ln),
		elastic.NewMatchPhraseQuery("ln.german", ln),
	).MinimumShouldMatch("1")

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

	if len(exclIDs) > 0 {
		for _, ID := range exclIDs {
			q.MustNot(elastic.NewMatchPhraseQuery("id", ID))
		}
	}

	// fn exactly fn1
	q.Must(elastic.NewMatchPhraseQuery("fn", strutils.FirstChar(fn)))

	nss := elastic.NewSearchSource().Query(q)

	searchResult, err := db.Search().Index("experts").Type("data").SearchSource(nss).From(0).Size(10).Do(context.Background())
	if err != nil {
		panic(err)
	}

	if searchResult.Hits.TotalHits == 0 {
		// fmt.Printf("[%s] %s %s %s \t\t ====> Not found\n", id, fn, mn, ln)
		return nil
	}

	if searchResult.Hits.TotalHits > 1 {
		fmt.Printf("[%s] \t\t ====> Too many results! Should be only one like: %s %s\n", id, strutils.FirstChar(fn), ln)
		return nil
	}

	for _, hit := range searchResult.Hits.Hits {
		var row map[string]interface{}

		err := json.Unmarshal(*hit.Source, &row)
		if err != nil {
			panic(err)
		}

		result = append(result, row)
	}

	return
}

func (db *DB) OneMiddleNameSearch(id, custName, fn, mn, ln, city string, did int, exclIDs []string) (result []map[string]interface{}) {
	q := elastic.NewBoolQuery().Filter(
		elastic.NewMatchPhraseQuery("did", did),
	).Should(
		elastic.NewMatchPhraseQuery("ln", ln),
		elastic.NewMatchPhraseQuery("ln.german", ln),
	).MinimumShouldMatch("1")

	fnq := elastic.NewBoolQuery().Should(
		elastic.NewMatchPhraseQuery("aliases", fn),
		elastic.NewMatchPhraseQuery("fn", fn),
	)
	fnq.Should().MinimumShouldMatch("1")
	q.Must(fnq)

	if mn != "" {
		// this case is only for the names with NO MN included

		// ---------------------------
		// ->Ralf    Dittrich {OSNABRÜCK}             ====> Found [did:1]: 5711743
		// 0.Ralf F. Dittrich {}
		//
		// We want to match this only if Ralf * Dittrich exist in OneKey Db only once!
		return nil
	}

	if len(exclIDs) > 0 {
		for _, ID := range exclIDs {
			q.MustNot(elastic.NewMatchPhraseQuery("id", ID))
		}
	}

	nss := elastic.NewSearchSource().Query(q)

	searchResult, err := db.Search().Index("experts").Type("data").SearchSource(nss).From(0).Size(10).Do(context.Background())
	if err != nil {
		panic(err)
	}

	if searchResult.Hits.TotalHits == 0 {
		// fmt.Printf("[%s] %s %s %s \t\t ====> Not found\n", id, fn, mn, ln)
		return nil
	}

	// if searchResult.Hits.TotalHits > 1 {
	// 	// fmt.Printf("[%s] \t\t ====> Too many results! (oneMiddleNameOnly1) %s %s\n", id, fn, ln)
	// 	// return nil
	// }

	for _, hit := range searchResult.Hits.Hits {
		var row map[string]interface{}

		err := json.Unmarshal(*hit.Source, &row)
		if err != nil {
			panic(err)
		}

		result = append(result, row)
	}

	return
}

func (db *DB) OneMiddleNameSearch2(id, custName, fn, mn, ln, city string, did int, exclIDs []string) (result []map[string]interface{}) {
	q := elastic.NewBoolQuery().Filter(
		elastic.NewMatchPhraseQuery("did", did),
		elastic.NewTermQuery("mn", ""),
	).Should(
		elastic.NewMatchPhraseQuery("ln", ln),
		elastic.NewMatchPhraseQuery("ln.german", ln),
	).MinimumShouldMatch("1")

	fnq := elastic.NewBoolQuery().Should(
		elastic.NewMatchPhraseQuery("aliases", fn),
		elastic.NewMatchPhraseQuery("fn", fn),
	)
	fnq.Should().MinimumShouldMatch("1")
	q.Must(fnq)

	if mn == "" {
		// this case is only for the names WITH MN included

		// ---------------------------
		// ->Ralf F. Dittrich {OSNABRÜCK}             ====> Found [did:1]: 5711743
		// 0.Ralf    Dittrich {}
		//
		// We want to match this only if Ralf * Dittrich exist in OneKey Db only once!
		return nil
	}

	if len(exclIDs) > 0 {
		for _, ID := range exclIDs {
			q.MustNot(elastic.NewMatchPhraseQuery("id", ID))
		}
	}

	nss := elastic.NewSearchSource().Query(q)

	searchResult, err := db.Search().Index("experts").Type("data").SearchSource(nss).From(0).Size(10).Do(context.Background())
	if err != nil {
		panic(err)
	}

	if searchResult.Hits.TotalHits == 0 {
		// fmt.Printf("[%s] %s %s %s \t\t ====> Not found\n", id, fn, mn, ln)
		return nil
	}

	if searchResult.Hits.TotalHits > 1 {
		// fmt.Printf("[%s] \t\t ====> Too many results! (oneMiddleNameOnly2) %s %s\n", id, fn, ln)
		return nil
	}

	for _, hit := range searchResult.Hits.Hits {
		var row map[string]interface{}

		err := json.Unmarshal(*hit.Source, &row)
		if err != nil {
			panic(err)
		}

		result = append(result, row)
	}

	return
}

func (db *DB) TestSearch(id, custName, fn, mn, ln, city string, did int, exclIDs []string) (result []map[string]interface{}) {
	q := elastic.NewBoolQuery().Filter(
		elastic.NewMatchPhraseQuery("did", did),
		elastic.NewPrefixQuery("fn", strutils.FirstChar(fn)),
	).Should(
		elastic.NewMatchPhraseQuery("ln", ln),
		elastic.NewMatchPhraseQuery("ln.german", ln),
	).MinimumShouldMatch("1")

	nss := elastic.NewSearchSource().Query(q)

	searchResult, err := db.Search().Index("experts").Type("data").SearchSource(nss).From(0).Size(200).Do(context.Background())
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

// func (s *service) mnConflict(fn, mn, ln string, did int) bool {
// 	q := elastic.NewBoolQuery().Filter(
// 		elastic.NewMatchPhraseQuery("did", did),
// 		elastic.NewMatchPhraseQuery("ln", ln),
// 	)

// 	fn = strings.Replace(fn, ".", "", -1)
// 	mn = strings.Replace(mn, ".", "", -1)

// 	fnl := len(fn)
// 	mnl := len(mn)

// 	fn1q := elastic.NewBoolQuery()
// 	fn1q.Should(elastic.NewMatchPhraseQuery("fn", fn))
// 	if fnl > 1 {
// 		fn1q.Should(elastic.NewMatchPhraseQuery("fn", FirstChar(fn)))
// 	}
// 	fn1q.MinimumShouldMatch("1")

// 	q.Must(fn1q)
// 	q.MustNot(elastic.NewTermQuery("mn", ""))

// 	nss := elastic.NewSearchSource().Query(q)

// 	searchResult, err := s.es.Search().Index("experts").Type("data").SearchSource(nss).From(0).Size(10).Do(context.Background())
// 	if err != nil {
// 		panic(err)
// 	}

// 	if searchResult.Hits.TotalHits == 0 {
// 		// no results, no conflicts
// 		return false
// 	}

// 	for _, hit := range searchResult.Hits.Hits {
// 		var row map[string]interface{}

// 		err := json.Unmarshal(*hit.Source, &row)
// 		if err != nil {
// 			panic(err)
// 		}

// 		rmn := row["mn"].(string)
// 		rmnl := len(rmn)

// 		if mnl == 1 && rmnl == 1 {
// 			if mn != row["mn"] {
// 				// mn1 != rowMn1 => conflict
// 				return true
// 			}
// 		} else if mnl == 1 && FirstChar(rmn) != mn {
// 			return true
// 		} else if rmnl == 1 && FirstChar(mn) != rmn {
// 			return true
// 		} else if mnl > 1 && rmnl > 1 && mn != rmn {
// 			return true
// 		}

// 		// fmt.Printf("[%s] %s %s %s {%s}\t\t ====> \t [%s] %s, {%s} npi: %v, ttid: %v\n", id, fn, mn, ln, city, hit.Id, row["name"], row["city"], row["npi"], row["ttid"])
// 	}

// 	return false
// }

// func mnSubQuery(mn string) (q *elastic.BoolQuery) {
// 	q = elastic.NewBoolQuery()

// 	if mn == "" {
// 		q.Must(elastic.NewMatchPhraseQuery("mn", ""))

// 		return
// 	}

// 	// Łukasz
// 	q.Should(elastic.NewMatchPhraseQuery("mn", mn))
// 	// Ł
// 	q.Should(elastic.NewMatchPhraseQuery("mn", FirstChar(mn)))
// 	// Ł.
// 	q.Should(elastic.NewMatchPhraseQuery("mn", FirstChar(mn)+"."))
// 	q.MinimumShouldMatch("1")

// 	return
// }
func (db *DB) IndexExperts(experts []*modelsMysql.Experts, batchInsert int) (err error) {
	if batchInsert == 0 {
		batchInsert = 1000
	}
	fmt.Printf("Processing %d experts\n", len(experts))

	// move below to a separate function
	p, err := db.BulkProcessor().Name("bdWorker").
		// Stats(true). // enable collecting stats
		Workers(2).
		BulkActions(batchInsert). // commit if # requests >= 1000
		// BulkSize(2 << 20).               // commit if size of requests >= 2 MB
		// FlushInterval(30 * time.Second). // commit every 30s
		// Before(beforeCallback). // func to call before commits
		// After(afterCallback).   // func to call after commits
		Do(context.Background())

	if err != nil {
		return
	}

	defer p.Close() // don't forget to close me

	// inserting to ES
	for _, expert := range experts {
		r := elastic.NewBulkIndexRequest().Index("experts").Type("data").Id(strconv.Itoa(expert.ID)).Doc(expert)

		// Add the request r to the processor p
		p.Add(r)

	}

	// Get a snapshot of stats (always blank if not enabled--see above)
	// stats := p.Stats()

	// fmt.Printf("Number of times flush has been invoked: %d\n", stats.Flushed)
	// fmt.Printf("Number of times workers committed reqs: %d\n", stats.Committed)
	// fmt.Printf("Number of requests indexed            : %d\n", stats.Indexed)
	// fmt.Printf("Number of requests reported as created: %d\n", stats.Created)
	// fmt.Printf("Number of requests reported as updated: %d\n", stats.Updated)
	// fmt.Printf("Number of requests reported as success: %d\n", stats.Succeeded)
	// fmt.Printf("Number of requests reported as failed : %d\n", stats.Failed)

	// for i, w := range stats.Workers {
	// 	fmt.Printf("Worker %d: Number of requests queued: %d\n", i, w.Queued)
	// 	fmt.Printf("           Last response time       : %v\n", w.LastDuration)
	// }

	// to flush the bulkprocessr manualy | otherwise use the params BulkActions, BulkSize or FlushInterval to flush
	// err = p.Flush()
	// checkErr(err)
	return
}
