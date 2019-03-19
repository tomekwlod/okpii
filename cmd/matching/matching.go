package main

/**

MATCHING UMLAUT!!
https://discuss.elastic.co/t/u-umlaut-search-indexing-user-name-muller-search-fails-for-muller-but-success-for-muller/60317/5
https://discuss.elastic.co/t/folding-german-characters-like-umlauts/3720/7
Kai Hübel <-- in onkey
1262013: Kai Huebel <-- mysql
1267961: Kai Hübel <-- mysql


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
	"flag"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"time"

	_ "github.com/go-sql-driver/mysql"
	modelsES "github.com/tomekwlod/okpii/models/es"
	modelsMongodb "github.com/tomekwlod/okpii/models/mongodb"
	modelsMysql "github.com/tomekwlod/okpii/models/mysql"
	"github.com/tomekwlod/okpii/tools"
	strutils "github.com/tomekwlod/utils/strings"
	_ "golang.org/x/net/html/charset"
)

const collectFromEveryStep = true

type service struct {
	es    modelsES.Repository
	mysql modelsMysql.Repository
	mongo modelsMongodb.Repository
	// logger  *log.Logger
}

func main() {
	didFlag := flag.String(
		"did",
		"1,2,3,9,10,11,12,13,14,15,16,17,22,24,25,26,27,28,29,30,31,32",
		"A deployments list comma separated of a single deployment")

	singleOKFlag := flag.String(
		"onekey",
		"",
		"Pass single OneKey to investigate it")

	// once done with the flags/arguments let's parse them
	flag.Parse()

	// grab deployments from an argument[1] - comma separated string
	deployments, err := tools.Deployments(*didFlag)
	if err != nil {
		panic(err)
	}
	fmt.Printf("\n> Starting with: %v deployment(s)\n", deployments)

	singleOK := *singleOKFlag
	if singleOK != "" {
		fmt.Printf("\n> Checking only one key: %s\n\n", singleOK)
	}

	t1 := time.Now()
	var wg sync.WaitGroup

	esClient, err := modelsES.ESClient()
	if err != nil {
		panic(err)
	}

	mysqlClient, err := modelsMysql.MysqlClient()
	if err != nil {
		panic(err)
	}
	defer mysqlClient.Close()

	mongoClient, err := modelsMongodb.MongoDB()
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
	go s.mongo.Onekeys(ch)

	var i int
	for m := range ch {
		i++

		fn, mn, ln := names(m)

		if singleOK != "" {
			// to test only one person
			if m["SRC_CUST_ID"] != singleOK {
				continue
			} else {
				fmt.Printf("%s :: %s :: %s", fn, mn, ln)
			}
		}

		for _, did := range deployments {
			did, _ := strconv.Atoi(did)

			result := s.findMatches(did, m["SRC_CUST_ID"], m["CUST_NAME"], m["CNTRY"], m["CITY"], fn, mn, ln)
			// _, matches := s.findMatches(did, m["SRC_CUST_ID"], m["City"], fn, mn, ln)

			for queryNumber, matches := range result {
				for _, match := range matches {
					if match["id"] != nil {
						kid64 := match["id"].(float64)
						kid := int(kid64)

						if kid > 0 {
							fmt.Printf("{q%d}: [%s] %s %s %s {%s}\t\t ====> \t [%d] (did:%d) %s, {%s} npi: %v, ttid: %v\n",
								queryNumber, m["SRC_CUST_ID"], fn, mn, ln, m["CITY"],
								kid, did, match["name"], match["city"], match["npi"], match["ttid"],
							)

							wg.Add(1)
							go s.mysql.AddOnekeyToKOL(&wg, kid, did, m["SRC_CUST_ID"])

						} else {
							fmt.Println("ID NOT VALID ", match["id"], kid)
						}
					}
				}
			}
		}
	}

	t2 := time.Now()

	wg.Wait()
	fmt.Printf("\nAll done in: %v \n", t2.Sub(t1))
}

// func (s *service) findMatches(did int, id, custName, city, fn, mn, ln string) (queryNumber int, result []map[string]interface{}) {
func (s *service) findMatches(did int, id, custName, country, city, fn, mn, ln string) (result map[int][]map[string]interface{}) {
	// this cannot seat in the return definition because it will panic below [assignment to entry in nil map]
	result = map[int][]map[string]interface{}{}

	if strings.Replace(fn, " ", "", -1) == "" {
		// if no FN we should just continue; it causes too much hassle
		return
	}

	var ids []string
	var midres []map[string]interface{}
	var noq = 5 // number of queries

	for i := 1; i <= noq; i++ {
		// for _, queryNumber := range []int{1, 2, 3, 4, 5} {

		midres = s.search(i, id, custName, fn, mn, ln, country, city, did, ids) //deployment=XX

		// before, it was a return when we had a match inside this for-loop
		// I introduced another for-loop underneath to append all the results from every search step
		// this may bring more matches but at the same time it is more risky
		//
		// the const collectFromEveryStep=false is to switch off this behaviour if needed

		if collectFromEveryStep == false {
			if len(midres) == 0 {
				// no results here, continue with another search
				continue
			}

			for _, row := range midres {
				result[i] = append(result[i], row)
			}

			// a match -> return with the matches from one search only
			return
		}

		// this can happen only if const collectFromEveryStep=true -> so it will collect the results from
		// every single search step
		for _, row := range midres {
			ids = append(ids, strconv.FormatFloat(row["id"].(float64), 'f', 0, 64))

			result[i] = append(result[i], row)
		}
	}

	return
}

func names(m map[string]string) (fn, mn, ln string) {
	fn = m["FIRST_NAME"]
	mn = ""
	ln = m["LAST_NAME"]

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

func (s *service) search(option int, id, custName, fn, mn, ln, country, city string, did int, exclIDs []string) (result []map[string]interface{}) {
	switch option {
	case 1:
		return s.es.SimpleSearch(id, custName, fn, mn, ln, country, city, did, exclIDs)
	case 2:
		return s.es.ShortSearch(id, custName, fn, mn, ln, country, city, did, exclIDs)
	case 3:
		r := s.es.NoMiddleNameSearch(id, custName, fn, mn, ln, country, city, did, exclIDs)

		// for security reason - double checking if the match is the only one in the DB
		for _, row := range r {
			total := s.mongo.CountOneKeyOcc(custName, strutils.FirstChar(fn), ln)

			if total != 0 {
				// fmt.Printf("[%s] %s %s %s {%s}\t\t ====> \t TOO MANY RESULTS FOR {q%v}!\n", id, fn, mn, ln, city, option)
				continue
			}

			result = append(result, row)
		}

		return result
	case 4:
		r := s.es.OneMiddleNameSearch(id, custName, fn, mn, ln, country, city, did, exclIDs)

		unique := map[string]string{}
		for _, row := range r {
			// the unique doesn't need to be based on the full names
			// ES matching is already doing the FN matching so here all we have to do is
			// to check the middle name and fn1 to be sure it is unique for our needs
			key := fmt.Sprintf("%s%s", strutils.FirstChar(row["fn"].(string)), row["mn"])
			unique[key] = key
		}
		if len(unique) > 1 {
			// if we have non unique matches in this already risky matching
			// we should not continue

			// @todo:
			// Frank G
			// Frank G       // these to will be ok but not these ones:

			// Frank G
			// Frank George  // this should also be ok I believe
			return nil
		}

		// for security reason - double checking if the match is the only one in the DB
		for _, row := range r {
			total := s.mongo.CountOneKeyOcc(custName, fn, ln)

			if total != 0 {
				// fmt.Printf("[%s] %s %s %s {%s}\t\t ====> \t TOO MANY RESULTS FOR {q%v}!\n", id, fn, mn, ln, city, option)
				continue
			}

			result = append(result, row)
		}

		return result
	case 5:
		r := s.es.OneMiddleNameSearch2(id, custName, fn, mn, ln, country, city, did, exclIDs)

		// for security reason - double checking if the match is the only one in the DB
		for _, row := range r {
			total := s.mongo.CountOneKeyOcc(custName, fn, ln)

			if total != 0 {
				// fmt.Printf("[%s] %s %s %s {%s}\t\t ====> \t TOO MANY RESULTS FOR {q%v}!\n", id, fn, mn, ln, city, option)
				continue
			}

			result = append(result, row)
		}

		return result
	default:
		return s.es.TestSearch(id, custName, fn, mn, ln, country, city, did, exclIDs)
		return nil
	}
}
