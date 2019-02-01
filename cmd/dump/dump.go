package main

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

const cdid = 2

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
	FNDash            bool     `json:"fnDash"`
	FNDot             bool     `json:"fnDot"`
	Country           string   `json:"country"`
	City              string   `json:"city"`
	Aliases           []string `json:"aliases"`
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

	for _, row := range s.experts(cdid) { //deployment=XX
		ir, err := s.es.Index().Index("experts").Type("data").Id(strconv.Itoa(row.ID)).BodyJson(row).Do(context.Background())
		if err != nil {
			panic(err)
		}

		fmt.Println(ir.Id, ir.Result)
	}
}

func (s *service) experts(did int) (result []*Experts) {
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
group by k.id`, did)
	if err != nil {
		panic(err.Error())
	}

	for rows.Next() {
		row := fill(rows)

		result = append(result, row)
	}

	return
}

func fill(rows *sql.Rows) (e *Experts) {
	var id, did, position int // if nullable then if should be sql.NullInt64
	var npi, ttid sql.NullInt64
	var fn, mn, ln, city, country sql.NullString // not just string here because of nulls
	var fn1, fn2, fn3, fn4 sql.NullString

	err := rows.Scan(&id, &fn, &ln, &mn, &npi, &ttid, &did, &position, &city, &country, &fn1, &fn2, &fn3, &fn4)
	if err != nil {
		panic(err.Error())
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
		Name:              name,
		NameKeyword:       name,
		NameKeywordSquash: squash,
		NameKeywordRaw:    squash,
		Deleted:           0,
		FNDash:            strings.Contains(fn.String, "-"),
		FNDot:             strings.Contains(fn.String, "."),
		Position:          position,
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

// func stringToAsciiBytes(str string) string {
// 	b := make([]byte, len(str))

// 	t := transform.Chain(norm.NFD, transform.RemoveFunc(isMn), norm.NFC)
// 	_, _, e1 := t.Transform(b, []byte(str), true)

// 	if e1 != nil {
// 		fmt.Println("String '" + str + "' couldn't be converted to ASCII")
// 		return ""
// 	}

// 	return string(b)
// }
