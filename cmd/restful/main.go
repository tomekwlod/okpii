package main

import (
	"io"
	"log"
	"net/http"
	"os"

	_ "github.com/go-sql-driver/mysql"
	"github.com/gorilla/context"
	"github.com/justinas/alice"
	"github.com/tomekwlod/okpii/models"
	modelsES "github.com/tomekwlod/okpii/models/es"
	modelsMysql "github.com/tomekwlod/okpii/models/mysql"
)

// service struct to hold the db and the logger
type service struct {
	es     modelsES.Repository
	mysql  modelsMysql.Repository
	logger *log.Logger
}

func main() {

	// definig the logger & a log file
	file, err := os.OpenFile("log/http.log", os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0666)
	if err != nil {
		log.Fatalln("Failed to open log file", err)
	}
	multi := io.MultiWriter(file, os.Stdout)
	l := log.New(multi, "", log.Ldate|log.Ltime|log.Lshortfile)

	esClient, err := modelsES.ESClient()
	if err != nil {
		log.Fatalln("Failed to connect to ES", err)
	}

	mysqlClient, err := modelsMysql.MysqlClient()
	if err != nil {
		log.Fatalln("Failed to connect to MySQL", err)
	}
	defer mysqlClient.Close()

	s := &service{
		es:     esClient,
		mysql:  mysqlClient,
		logger: l,
	}

	commonHandlers := alice.New(context.ClearHandler, s.loggingHandler, recoverHandler, acceptHandler)
	optionsHandlers := alice.New(context.ClearHandler, s.loggingHandler)

	router := NewRouter()

	// dump experts for one deployment
	router.Get("/dump/:did", commonHandlers.ThenFunc(s.dumpHandler))
	// counter
	router.Get("/experts/:did", commonHandlers.ThenFunc(s.expertsHandler))
	// create
	router.Post("/match", commonHandlers.Append(contentTypeHandler, bodyHandler(models.Expert{})).ThenFunc(s.matchHandler))
	// delete
	router.Delete("/expert/:id", commonHandlers.ThenFunc(s.deleteHandler))
	// update
	router.Put("/expert/:id", commonHandlers.Append(contentTypeHandler, bodyHandler(models.Expert{})).ThenFunc(s.updateHandler))

	// cors support
	router.Options("/*name", optionsHandlers.ThenFunc(allowCorsHandler))

	l.Printf("Server started and listening on port %s. Ready for the requests.\n\n", "7171")
	if err := http.ListenAndServe(":7171", router); err != nil {
		l.Panic("Error occured: ", err)
	}
}
