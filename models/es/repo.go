package models

import (
	"context"
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
	"text/scanner"
	"unicode"

	"github.com/tomekwlod/okpii/models"
	modelsMysql "github.com/tomekwlod/okpii/models/mysql"
	strutils "github.com/tomekwlod/utils/strings"
	elastic "gopkg.in/olivere/elastic.v6"
)

var countryCodes = map[string]string{
	// "AND": "Andorra",
	// "AUS": "Australia",
	"AUT": "Austria",
	"BEL": "Belgium",
	"CHE": "Switzerland", //"Seychelles"
	// "CZE": "Czech Republic",
	"DEU": "Germany",
	"DNK": "Denmark",
	"ESP": "Spain",
	// "EST": "Estonia",
	"FIN": "Finland",
	"FRA": "France",
	// "FRO": "Faroe Islands",
	"GBR": "United Kingdom",
	// "GLP": "Guadeloupe",
	// "GRL": "Greenland",
	// "GUF": "French Guiana",
	// "HRV": "Croatia",
	// "HUN": "Hungary",
	"IRL": "Ireland",
	"ITA": "Italy",
	// "LTU": "Lithuania",
	// "LUX": "Luxembourg",
	// "LVA": "Latvia",
	// "MCO": "Monaco",
	// "MTQ": "Martinique",
	// "MYT": "Mayotte",
	// "NCL": "New Caledonia",
	"NLD": "Netherlands",
	"NOR": "Norway",
	// "NZL": "New Zealand",
	// "POL": "Poland",
	"PRT": "Portugal",
	// "PYF": "French Polynesia",
	// "REU": "Reunion",
	// "SPM": "Saint Pierre and Miquelon",
	// "SVK": "Slovakia",
	// "SVN": "Slovenia",
	"SWE": "Sweden",
	// "TUR": "Turkey",
	"WLF": "Wallis and Futuna",
}

func baseQuery(did int, country string, exclIDs []string) (*elastic.BoolQuery, error) {
	q := elastic.NewBoolQuery().Filter(
		elastic.NewMatchPhraseQuery("did", did),
	)

	if country != "" {
		if val, ok := countryCodes[country]; ok {
			// if country provided, use it, otherwise ignore the country at all
			q.Filter(elastic.NewMatchPhraseQuery("country", val))
		} else {
			return nil, fmt.Errorf("Country code %s not defined", country)
		}
	}

	// main id to be excluded if passed
	if len(exclIDs) > 0 {
		for _, ID := range exclIDs {
			q.MustNot(elastic.NewMatchPhraseQuery("id", ID))
		}
	}

	q.Must(elastic.NewMatchPhraseQuery("deleted", 0))

	return q, nil
}
func lastNameQuery(q *elastic.BoolQuery, ln string) *elastic.BoolQuery {
	// simple := elastic.NewMatchPhraseQuery("ln", ln)
	var qs []elastic.Query

	qs = append(qs, elastic.NewMatchPhraseQuery("ln", ln))

	if !strutils.IsASCII(ln) {
		// @todo: test how many fewer results we have! with:3= without:3=
		qs = append(qs, elastic.NewMatchPhraseQuery("ln.german", ln))
	}

	q.Should(
		qs...,
	).MinimumShouldMatch("1")

	return q
}

// BaseQuery just returns the base query so you can use it for your specifin needs
func (db *DB) BaseQuery(did int, country string, exclIDs []string) (*elastic.BoolQuery, error) {
	return baseQuery(did, country, exclIDs)
}
func (db *DB) ExecuteQuery(q *elastic.BoolQuery) (result []map[string]interface{}, err error) {
	nss := elastic.NewSearchSource().Query(q)

	searchResult, err := db.Search().Index("experts").Type("data").SearchSource(nss).From(0).Size(100).Do(context.Background())
	if err != nil {
		return
	}

	if searchResult.Hits.TotalHits > 0 {
		for _, hit := range searchResult.Hits.Hits {
			var row map[string]interface{}

			err := json.Unmarshal(*hit.Source, &row)
			if err != nil {
				panic(err)
			}

			result = append(result, row)
		}
	}

	return
}

func (db *DB) SimpleSearch(fn, mn, ln, country, city string, did int, exclIDs []string) []map[string]interface{} {
	result := []map[string]interface{}{}

	q, err := baseQuery(did, country, exclIDs)
	if err != nil {
		return nil
	}

	// rln := strutils.LastWord(ln)

	mnstr := " "
	mn1 := ""
	if mn != "" {
		mnstr = " " + mn + " "
		mn1 = " " + strutils.FirstChar(mn) + " "
	}

	name := fn + mnstr + ln
	nameRaw := strings.Replace(strings.Replace(name, " ", "", -1), "-", "", -1)
	name1 := fn + mn1 + ln

	// if len(nameRaw) <= 4 {
	// 	// to avoid very weak merging and to temporary eliminate:
	// 	// Ma  Li
	// 	// M  Ali
	// 	// or
	// 	// S  Yan
	// 	// S Y An
	// 	return nil
	// }

	// nameKeyword is case sensitive
	// nameKeywordSquash,nameKeywordRaw is NOT case sensitive
	if len(nameRaw) > 4 {
		// JohnMarkSmith || BrianSurni    <- with ASCII-folding
		q.Should(elastic.NewTermQuery("nameKeywordSquash", nameRaw))
		// JohnMarkSmith || BrianSurni    <- with ASCII-folding & lowercase
		q.Should(elastic.NewTermQuery("nameKeywordRaw", nameRaw))

	}

	// John Mark Smith || Brian Surni <- with ASCII-folding
	q.Should(elastic.NewTermQuery("nameKeyword", name))

	// only if the given name is with nonASCII we should add the german(and other) languages support
	// if !strutils.IsASCII(name) {
	// @todo: test how many less results we have!
	q.Should(elastic.NewTermQuery("nameKeyword.german", name))
	// }

	if name1 != name {
		// John M Smith <- with ASCII-folding
		q.Should(elastic.NewTermQuery("nameKeyword", name1))

		if len(nameRaw) > 4 {
			// JohnMSmith   <- with ASCII-folding
			nameRaw = strings.Replace(strings.Replace(name1, " ", "", -1), "-", "", -1)
			q.Should(elastic.NewTermQuery("nameKeywordSquash", nameRaw))
		}
	}

	q.MinimumShouldMatch("1")

	// disabled because we would not match a lot of good examples, eg:
	//  Joaquín Pastor Fernández
	//  Joaquín Pastor Ferná Ndez
	// ... needs more thinking
	//
	// // this is to avoid matching:
	// //  Ma  Li
	// //  M  Ali
	// // or
	// //  S  Yan
	// //  S Y An
	// q.Must(elastic.NewMatchPhraseQuery("name", rln))

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

	// if searchResult.Hits.TotalHits > 2 {
	// 	fmt.Printf("\n\n\n!!!!!! (simple)  Too many (%d) results %s;\n\n\n", searchResult.Hits.TotalHits, name)
	// 	// fmt.Printf("[%s] %s %s %s\n", id, fn, mn, ln)

	// 	// for _, hit := range searchResult.Hits.Hits {
	// 	// 	var row map[string]interface{}

	// 	// 	err := json.Unmarshal(*hit.Source, &row)
	// 	// 	if err != nil {
	// 	// 		panic(err)
	// 	// 	}
	// 	// 	// fmt.Printf(" > [%s] %s %s %s\n", hit.Id, row["fn"], row["mn"], row["ln"])
	// 	// }
	// 	return nil
	// }

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

func (db *DB) ShortSearch(fn, mn, ln, country, city string, did int, exclIDs []string) (result []map[string]interface{}) {
	if mn == "" {
		// this case is only for the names with MN included
		return nil
	}

	q, err := baseQuery(did, country, exclIDs)
	if err != nil {
		return nil
	}

	// adding LN to a query
	q = lastNameQuery(q, ln)

	fn = strings.Replace(fn, ".", "", -1)
	mn = strings.Replace(mn, ".", "", -1)

	fnl := len(fn)
	mnl := len(mn)

	if fnl <= 1 && mnl <= 1 {
		// nothing to do if short names already; squash would do the job
		// J E Cortes
		// or
		// J Cortes
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
		mn1q.Should(elastic.NewPrefixQuery("mn", mn))
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
		fmt.Printf("  !!!!!! (short) Too many (%d) results %s;\n", searchResult.Hits.TotalHits, fmt.Sprintf("%s %s %s", fn, mn, ln))
		// fmt.Printf("[%s] %s %s %s\n", id, fn, mn, ln)

		// for _, hit := range searchResult.Hits.Hits {
		// 	var row map[string]interface{}

		// 	err := json.Unmarshal(*hit.Source, &row)
		// 	if err != nil {
		// 		panic(err)
		// 	}
		// 	// fmt.Printf(" > [%s] %s %s %s\n", hit.Id, row["fn"], row["mn"], row["ln"])
		// }
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

func (db *DB) NoMiddleNameSearch(fn, mn, ln, country, city string, did int, exclIDs []string) (result []map[string]interface{}) {
	// this case is only for the names with NO MN on both sides!!
	//
	// EXPLANATION
	//
	// For a given Jorge  Cortes
	// find all    J      Cortes
	// or
	// For a given J      Cortes
	// find all    J*     Cortes

	// WARNING! IMPORTANT! Check the quantity of the results later!!

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

	q, err := baseQuery(did, country, exclIDs)
	if err != nil {
		return nil
	}

	// adding LN to a query
	q = lastNameQuery(q, ln)

	q.Filter(
		elastic.NewTermQuery("mn", ""),
	)

	// fn exactly fn1
	if len(fn) > 1 {
		q.Must(elastic.NewMatchPhraseQuery("fn", strutils.FirstChar(fn)))
	} else {
		q.Must(elastic.NewPrefixQuery("fn", strutils.FirstChar(fn)))
	}
	nss := elastic.NewSearchSource().Query(q)

	searchResult, err := db.Search().Index("experts").Type("data").SearchSource(nss).From(0).Size(10).Do(context.Background())
	if err != nil {
		panic(err)
	}

	if searchResult.Hits.TotalHits == 0 {
		return nil
	}

	if searchResult.Hits.TotalHits > 1 {
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

func (db *DB) OneMiddleNameSearch(fn, mn, ln, country, city string, did int, exclIDs []string) (result []map[string]interface{}) {
	// this case is only for the names with NO MN incomming
	//
	// EXPLANATION
	//
	// For a given Jorge    Cortes
	// find all    Jorge X* Cortes  names
	//
	// WARNING! Check later if there is more than one result here

	if mn != "" {
		// ---------------------------
		// ->Ralf    Dittrich {OSNABRÜCK}             ====> Found [did:1]: 5711743
		// 0.Ralf F. Dittrich {}
		//
		// We want to match this only if Ralf * Dittrich exist in OneKey Db only once!
		return nil
	}

	q, err := baseQuery(did, country, exclIDs)
	if err != nil {
		return nil
	}

	// adding LN to a query
	q = lastNameQuery(q, ln)
	q.MustNot(elastic.NewTermQuery("mn", ""))

	fnq := elastic.NewBoolQuery().Should(
		elastic.NewMatchPhraseQuery("aliases", fn),
		elastic.NewMatchPhraseQuery("fn", fn),
	)
	fnq.Should().MinimumShouldMatch("1")
	q.Must(fnq)

	nss := elastic.NewSearchSource().Query(q)

	searchResult, err := db.Search().Index("experts").Type("data").SearchSource(nss).From(0).Size(10).Do(context.Background())
	if err != nil {
		panic(err)
	}

	if searchResult.Hits.TotalHits == 0 {
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

func (db *DB) OneMiddleNameSearch2(fn, mn, ln, country, city string, did int, exclIDs []string) (result []map[string]interface{}) {

	// this case is only for the names WITH MN included
	//
	// EXPLANATION
	//
	// For a given  Jorge E Cortes
	// find all     Jorge   Cortes  names only
	//
	// WARNING! Check later if we have no Jorge !E Cortes

	if mn == "" {
		// ---------------------------
		// ->Ralf F. Dittrich {OSNABRÜCK}             ====> Found [did:1]: 5711743
		// 0.Ralf    Dittrich {}
		//
		// We want to match this only if Ralf * Dittrich exist in OneKey Db only once!
		return nil
	}

	q, err := baseQuery(did, country, exclIDs)
	if err != nil {
		return nil
	}

	// adding LN to a query
	q = lastNameQuery(q, ln)

	q.Filter(
		elastic.NewTermQuery("mn", ""),
	)

	fnq := elastic.NewBoolQuery().Should(
		elastic.NewMatchPhraseQuery("aliases", fn),
		elastic.NewMatchPhraseQuery("fn", fn),
	)
	fnq.Should().MinimumShouldMatch("1")
	q.Must(fnq)

	nss := elastic.NewSearchSource().Query(q)

	searchResult, err := db.Search().Index("experts").Type("data").SearchSource(nss).From(0).Size(10).Do(context.Background())
	if err != nil {
		panic(err)
	}

	if searchResult.Hits.TotalHits == 0 {
		// fmt.Printf("[%s] %s %s %s \t\t ====> Not found\n", id, fn, mn, ln)
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

func (db *DB) ThreeInitialsSearch(fn, mn, ln, country, city string, did int, exclIDs []string) (result []map[string]interface{}) {
	if mn != "" || len(fn) <= 1 {
		// this case is only for the names with NO MN included
		// also first name needs to be longer than 1 character

		// ---------------------------
		// -> GJ     OSSENKOPPELE
		// 0. Gert J OSSENKOPPELE
		return nil
	}

	q, err := baseQuery(did, country, exclIDs)
	if err != nil {
		return nil
	}

	// adding LN to a query
	q = lastNameQuery(q, ln)

	q.Filter(
		elastic.NewPrefixQuery("fn", strutils.FirstChar(fn)),
	)

	nss := elastic.NewSearchSource().Query(q)

	searchResult, err := db.Search().Index("experts").Type("data").SearchSource(nss).From(0).Size(10).Do(context.Background())
	if err != nil {
		panic(err)
	}

	if searchResult.Hits.TotalHits == 0 {
		// fmt.Printf("[%s] %s %s %s \t\t ====> Not found\n", id, fn, mn, ln)
		return nil
	}

	for _, hit := range searchResult.Hits.Hits {
		var row map[string]interface{}

		err := json.Unmarshal(*hit.Source, &row)
		if err != nil {
			panic(err)
		}

		// checking the initials
		var initials []string
		for _, char := range row["fn"].(string) + row["mn"].(string) {
			if !unicode.IsLower(char) && char != ' ' {
				var s string
				s = scanner.TokenString(char)
				s = s[1 : len(s)-1] // this removed the quotes around the string

				initials = append(initials, s)
			}
		}

		if strings.Join(initials, "") == fn {
			result = append(result, row)
		}
	}

	return
}

func (db *DB) TestSearch(fn, mn, ln, country, city string, did int, exclIDs []string) (result []map[string]interface{}) {
	q, err := baseQuery(did, country, exclIDs)
	if err != nil {
		return nil
	}

	// adding LN to a query
	q = lastNameQuery(q, ln)

	q.Filter(
		elastic.NewPrefixQuery("fn", strutils.FirstChar(fn)),
	)

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

		fmt.Printf("\n\n---------------------------\n->%s %s %s {%s} \t\t ====> Found [did:%d]: %s\n%s \n\n", fn, mn, ln, city, did, strings.Join(ids, ","), strings.Join(n, "\n"))
	} else {
		// fmt.Printf("{q%d} [%s] %s %s %s \t\t ====> Not found\n", i, id, fn, mn, ln)
	}

	return
}

func (db *DB) Count(did int) (count int) {
	q, err := baseQuery(did, "", nil)
	if err != nil {
		panic(err)
	}

	nss := elastic.NewSearchSource().Query(q)

	searchResult, err := db.Search().Index("experts").Type("data").SearchSource(nss).From(0).Size(10).Do(context.Background())
	if err != nil {
		panic(err)
	}

	hits := searchResult.Hits.TotalHits

	return int(hits)
}

func (db *DB) FindOne(id, did int, ln string) (expert models.Expert, err error) {
	q, err := baseQuery(did, "", nil)
	if err != nil {
		panic(err)
	}

	q.Must(elastic.NewMatchPhraseQuery("id", id))
	q.Must(elastic.NewMatchPhraseQuery("ln", ln))

	nss := elastic.NewSearchSource().Query(q)

	searchResult, err := db.Search().Index("experts").Type("data").SearchSource(nss).From(0).Size(10).Do(context.Background())
	if err != nil {
		panic(err)
	}

	hits := searchResult.Hits.TotalHits
	if hits == 0 {
		return expert, fmt.Errorf("Couldn't find requested expert: [%d] %s", id, ln)
	}
	if hits > 1 {
		return expert, fmt.Errorf("More than one hit is not allowed here: hits: %d, [%d] %s", hits, id, ln)
	}

	for _, hit := range searchResult.Hits.Hits {
		err := json.Unmarshal(*hit.Source, &expert)
		if err != nil {
			panic(err)
		}
	}

	return
}

func (db *DB) RemoveData(did int) (deleted int64, err error) {
	del, err := db.DeleteByQuery("experts").Query(elastic.NewMatchPhraseQuery("did", did)).Do(context.TODO())

	// move below to a separate function
	// deleteIndex, err := db.DeleteIndex("experts").Do(context.TODO())

	if err != nil {
		return
	}

	if del == nil {
		return 0, fmt.Errorf("expected response; got: %+v", del)
	}

	return del.Deleted, nil
}

func (db *DB) MarkAsDeleted(id string) (err error) {
	_, err = db.Update().Index("experts").Type("data").Id(id).Doc(map[string]int{"deleted": 1}).Do(context.TODO())

	if err != nil {
		return
	}

	return
}
func (db *DB) UpdatePartially(id string, exp models.Expert) (err error) {
	_, err = db.Update().Index("experts").Type("data").Id(id).Doc(exp).Do(context.TODO())

	if err != nil {
		return
	}

	return
}

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
