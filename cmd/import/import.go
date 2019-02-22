package main

/*
IMPORT
Imports a CSV file to MongoDB database
*/

import (
	"bufio"
	"errors"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/mongodb/mongo-go-driver/bson"
	"github.com/mongodb/mongo-go-driver/mongo"

	modelsMongodb "github.com/tomekwlod/okpii/models/mongodb"
)

/*
This import in batches of X (see const below) inserts the data from CSV file to MongoDB. The only thing I would change
is the actual import; the main for loop through 'ch' should not wait for the Mongo to import the data but continue to
prepare and build the operations needed for the next one. Other than that looks fine (the program is agnostic to the size
of the CSV file so it should in theory import every file)

@todo: check how the empty lines in CSV will be dealt
@todo: the db/collection values are hardcoded! changeme!!
@todo: file source is also hardcoded!! changeme!!
@todo: Clean() should be exported as a util, outside of this command!

Nice article about dealing with huuuge CSV files and combining the lines one by one
https://danrl.com/blog/2018/merging-huuuge-csv-files-using-golang-channels/

Tutorial about how to use the mongo-go-driver:
https://www.mongodb.com/blog/post/mongodb-go-driver-tutorial

*/

type service struct {
	mongo modelsMongodb.Repository
}

const batch = 3000
const filename = "./dump4-sc.csv"

// move me!!
const (
	bom0 = 0xef
	bom1 = 0xbb
	bom2 = 0xbf
)

// move me!!
// Clean function rtrims the string from the BOM characters
func Clean(b []byte) []byte {
	if len(b) >= 3 &&
		b[0] == bom0 &&
		b[1] == bom1 &&
		b[2] == bom2 {
		return b[3:]
	}
	return b
}

func main() {

	t1 := time.Now()

	// definging the mongodb session
	db, err := modelsMongodb.MongoDB()
	if err != nil {
		panic(err)
	}

	// combine the datastore session and the logger into one struct
	s := &service{
		mongo: db,
	}

	ch := make(chan []string) // one line from csv
	go csvReader(filename, ch)

	var operations []mongo.WriteModel
	headers := []string{}
	headersLine := true

	for line := range ch {
		// this happens only once for the headers line
		if headersLine {
			headers, err = validateHeader(line)
			if err != nil {
				panic(err)
			}

			headersLine = false
			continue
		}

		row := consolidateWithHeader(headers, line)

		t := true
		operation := mongo.NewReplaceOneModel()
		operation.Filter = bson.D{{"_id", row["SRC_CUST_ID"]}}
		operation.Upsert = &t
		operation.Replacement = row

		operations = append(operations, operation)

		if len(operations) >= batch {
			err := s.mongo.Flush(operations)
			if err != nil {
				panic(err)
			}

			//reset
			operations = []mongo.WriteModel{}
		}
	}

	err = s.mongo.Flush(operations)
	if err != nil {
		panic(err)
	}

	t2 := time.Now()

	fmt.Printf("All done in: %v \n", t2.Sub(t1))

}

func csvReader(fname string, out chan<- []string) {
	defer close(out)

	// Load a TXT file.
	f, err := os.Open(fname)
	if err != nil {
		panic(err)
	}
	defer f.Close()

	scanner := bufio.NewScanner(f)
	for scanner.Scan() {
		columns := strings.Split(scanner.Text(), ",")

		out <- columns
	}

	if err := scanner.Err(); err != nil {
		fmt.Fprintln(os.Stderr, "reading standard input:", err)
	}

}

func validateHeader(headers []string) (newHeaders []string, err error) {
	// required := []string{"SRC_CUST_ID", "CUST_NAME", "FIRST_NAME", "MIDDLE_NAME", "LAST_NAME", "SRC_FIRST_NAME", "SRC_LAST_NAME", "SRC_MIDDLE_NAME",
	// 	"OneKeyID_Address", "City", "ZIP", "State", "Country",
	// }
	required := []string{"SRC_CUST_ID", "CUST_NAME", "FIRST_NAME", "LAST_NAME", "CITY", "CNTRY"}

	for _, header := range headers {
		cl := Clean([]byte(header))
		header = string(cl)

		newHeaders = append(newHeaders, header)
	}

	missing := []string{}
	for _, field := range required {
		found := false

		for _, header := range newHeaders {
			if header == field {
				// it's ok, break the sub-for
				found = true

				break
			}
		}

		if !found {
			missing = append(missing, field)
		}
	}

	if len(missing) > 0 {
		err = errors.New("Missing columns: " + strings.Join(missing, ", "))
	}

	return
}

// Combines header with the lines and as an output we have ["column1" => "value1", "column2" => "value2"]
func consolidateWithHeader(headers, line []string) map[string]string {
	m := map[string]string{}

	for i, h := range headers {
		m[h] = line[i]
	}

	return m
}
