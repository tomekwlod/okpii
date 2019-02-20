package main

/*
DUMP
Dumps data from MySQL to Elasticsearch
*/

import (
	"context"
	"database/sql"
	"fmt"
	"strconv"
	"strings"

	_ "github.com/go-sql-driver/mysql"
	"github.com/tomekwlod/okpii/db"
	_ "golang.org/x/net/html/charset"
	elastic "gopkg.in/olivere/elastic.v6"
)

const cdid = "9"
const batchInsert = 1000

type service struct {
	es    *elastic.Client
	mysql *sql.DB
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
	esClient, err := db.ESClient()
	checkErr(err)

	mysqlClient, err := db.MysqlClient()
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
		var experts []*Experts // needs to stay here. If we do below: `err,lastID,experts := s.fetchExperts(...)` it will override id all the time instead of reusing the declared one above

		// for {
		// getting the experts from the MySQL
		lastID, experts, err = s.fetchExperts(lastID, did, batchInsert)
		checkErr(err)

		if len(experts) == 0 {
			break
		}

		// indexing the experts onto ES
		err = s.indexExperts(experts)
		checkErr(err)
		// }
	}
}

func (s *service) fetchExperts(id, did, batchLimit int) (newID int, result []*Experts, err error) {
	newID = id
	// later, if bigger queries: https://dev.to/backendandbbq/the-sql-i-love-chapter-one

	rows, err := s.mysql.Query(`
SELECT 
	k.id, k.first_name as fn, k.last_name as ln, k.middle_name as mn, k.npi, k.ttid, k.deployment_id as did, r.position, l.city, l.country_name as country, 
	GROUP_CONCAT(distinct ke.first_name SEPARATOR ' ;;; ') as fn1,
	GROUP_CONCAT(distinct cem.firstName SEPARATOR ' ;;; ') as fn2, 
	(select GROUP_CONCAT(distinct name SEPARATOR ' ;;; ') from firstname where nickname = k.first_name) as fn3, 
	(select GROUP_CONCAT(distinct nickname SEPARATOR ' ;;; ') from firstname where name = k.first_name) as fn4
FROM kol k
LEFT JOIN rank_score_kol r ON r.kol_id = k.id
left join kol__entry ke on ke.kol_id = k.id and length(ke.first_name ) > 1 and ke.first_name <> k.first_name
left join kol_embase kem on kem.kol_id = k.id
left join container__embase_entry cem on cem.id = kem.embase_entry_id and length(cem.firstName) > 1 and cem.firstName <> k.first_name
LEFT JOIN location l ON l.id = k.default_location_id
WHERE
	k.deployment_id = ?
	and l.country_name like "germany"
	AND k.id > ?
group by k.id
ORDER BY r.position ASC
LIMIT ?`, did, newID, batchLimit)

	if err != nil {
		return
	}

	for rows.Next() {
		row, err := transform(rows)
		if err != nil {
			return newID, result, err
		}

		result = append(result, row)

		newID = row.ID
	}

	return
}

func (s *service) indexExperts(experts []*Experts) (err error) {
	fmt.Printf("Processing %d experts\n", len(experts))

	// move below to a separate function
	p, err := s.es.BulkProcessor().Name("bdWorker").
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

func transform(rows *sql.Rows) (e *Experts, err error) {
	var id, did int // if nullable then if should be sql.NullInt64
	var npi, ttid, position sql.NullInt64
	var fn, mn, ln, city, country sql.NullString // not just string here because of nulls
	var fn1, fn2, fn3, fn4 sql.NullString

	err = rows.Scan(&id, &fn, &ln, &mn, &npi, &ttid, &did, &position, &city, &country, &fn1, &fn2, &fn3, &fn4)
	if err != nil {
		return
	}

	aliases := mergeAliases(fn1, fn2, fn3, fn4)

	mnstr := " "
	if mn.String != "" {
		mnstr = " " + mn.String + " "
	}
	name := "" + fn.String + mnstr + ln.String
	squash := strings.Replace(strings.Replace(name, "-", "", -1), " ", "", -1)

	e = &Experts{
		ID:                id,
		Did:               did,
		NPI:               int(npi.Int64),
		TTID:              int(ttid.Int64),
		Fn:                fn.String,
		Mn:                mn.String,
		Ln:                ln.String,
		Name:              name,
		NameKeyword:       name,
		NameKeywordSquash: squash,
		NameKeywordRaw:    squash,
		Deleted:           0,
		FNDash:            strings.Contains(fn.String, "-"),
		FNDot:             strings.Contains(fn.String, "."),
		Position:          int(position.Int64),
		City:              city.String,
		Country:           country.String,
		Aliases:           aliases,
	}

	return
}

func mergeAliases(fn1, fn2, fn3, fn4 sql.NullString) (aliases []string) {
	s1 := strings.Split(fn1.String, " ;;; ")
	s2 := strings.Split(fn2.String, " ;;; ")
	s3 := strings.Split(fn3.String, " ;;; ")
	s4 := strings.Split(fn4.String, " ;;; ")

	set := make(map[string]string)

	for _, s := range s1 {
		if s != "" {
			if strings.Contains(s, ".") && len(s) == 2 {
				continue
			}
			set[s] = s
		}
	}
	for _, s := range s2 {
		if s != "" {
			if strings.Contains(s, ".") && len(s) == 2 {
				continue
			}
			set[s] = s
		}
	}
	for _, s := range s3 {
		if s != "" {
			if strings.Contains(s, ".") && len(s) == 2 {
				continue
			}
			set[s] = s
		}
	}
	for _, s := range s4 {
		if s != "" {
			if strings.Contains(s, ".") && len(s) == 2 {
				continue
			}
			set[s] = s
		}
	}

	for _, s := range set {
		aliases = append(aliases, s)
	}

	return
}

func checkErr(err error) {
	if err != nil {
		panic(err)
	}
}
