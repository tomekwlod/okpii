package db

import (
	"database/sql"
	"fmt"
	"os"
)

func MysqlClient() (db *sql.DB, err error) {
	user := os.Getenv("MYSQL_USER")
	if user == "" {
		user = "user"
	}
	pass := os.Getenv("MYSQL_PASS")
	if pass == "" {
		pass = "pass"
	}
	host := os.Getenv("MYSQL_HOST")
	if host == "" {
		host = "localhost"
	}
	port := os.Getenv("MYSQL_PORT")
	if port == "" {
		port = "3306"
	}
	dbname := os.Getenv("MYSQL_DB")
	if dbname == "" {
		dbname = "database"
	}

	// s := fmt.Sprintf("Hi, my name is %s and I'm %d years old.", "Bob", 23)
	// s := fmt.Sprint("[age:", i, "]")
	set := fmt.Sprintf("%s:%s@tcp(%s:%s)/%s?charset=utf8",
		user, pass, host, port, dbname,
	)

	db, err = sql.Open("mysql", set)
	if err != nil {
		return
	}

	//defer in main body methods
	// defer db.Close()

	// Open doesn't open a connection. Validate DSN data:
	err = db.Ping()
	if err != nil {
		return
	}

	fmt.Println("Connection to MySQL established")

	return
}
