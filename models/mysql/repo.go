package models

import (
	"database/sql"
	"strings"
	"sync"

	strutils "github.com/tomekwlod/utils/strings"
)

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

func (db *DB) AddOnekeyToKOL(wg *sync.WaitGroup, id, did int, oneky string) (status int64, err error) {
	defer wg.Done()

	result, err := db.Exec("INSERT INTO kol__onekey SET onekey=?, kid=?, did=?", oneky, id, did)
	if err != nil {
		return
	}

	status, err = result.RowsAffected()

	return
}

func (db *DB) FetchExperts(id, did, batchLimit int, countries []string) (newID int, result []*Experts, err error) {
	newID = id
	// later, if bigger queries: https://dev.to/backendandbbq/the-sql-i-love-chapter-one

	var tmp = []string{}
	for _, country := range countries {
		tmp = append(tmp, " l.country_name LIKE \""+country+"\" ")
	}
	countriesQuery := ""
	if len(tmp) > 0 {
		countriesQuery = "AND (" + (strings.Join(tmp, " OR ")) + ") "
	}

	rows, err := db.Query(`
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
	`+countriesQuery+`
	AND k.id > ?
group by k.id
ORDER BY k.id ASC
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

	// this is a case when the middle name is either empty or contains a single letter
	// also first name has to contain a space or a dash
	//
	// basically it's trying to modify the middle name if better found in first name
	for _, separator := range []string{" ", "-"} {
		fne := strings.Split(fn.String, separator)
		if len(fne) > 1 {
			if mn.String == "" {
				fn.String = fne[0]
				mn.String = strings.Join(fne[1:], separator)

				break
			} else {
				if len(mn.String) == 1 && strutils.FirstChar(strings.Join(fne[1:], " ")) == mn.String {
					fn.String = fne[0]
					mn.String = strings.Join(fne[1:], separator)

					break
				}
			}
		}
	}
	// end of the middle name modification

	// only if fn doesn't include: `-` , `.` , ` `  . Above statement shoudl take care of them ^^^^
	if !strings.Contains(fn.String, " ") && !strings.Contains(fn.String, ".") && !strings.Contains(fn.String, "-") {
		for _, separator := range []string{" ", "-", "."} {
			mne := strings.Split(mn.String, separator)

			if len(mne) > 1 {
				if strutils.FirstChar(fn.String) == mne[0] {
					// Adam A.M. Smith ---> Adam M. Smith
					// Adam A Smith   !---> Adam Smith    <-- probably too risky
					mn.String = strings.Join(mne[1:], separator)
				}
			}
		}
	}
	// end of the middle name modification

	// remove the dots from the middle name
	if mn.String != "" {
		mn.String = strings.Replace(mn.String, ".", " ", -1)
	}

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
