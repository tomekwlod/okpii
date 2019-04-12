package main

import (
	"io"
	"log"
	"net/http"
	"os"

	"github.com/gorilla/context"
	"github.com/justinas/alice"
	modelsES "github.com/tomekwlod/okpii/models/es"
)

// service struct to hold the db and the logger
type service struct {
	es     modelsES.Repository
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

	s := &service{
		es:     esClient,
		logger: l,
	}

	commonHandlers := alice.New(context.ClearHandler, s.loggingHandler, recoverHandler, acceptHandler)
	optionsHandlers := alice.New(context.ClearHandler, s.loggingHandler)

	router := NewRouter()
	router.Get("/pages", commonHandlers.ThenFunc(s.pagesHandler))
	router.Get("/page/:id", commonHandlers.ThenFunc(s.pageHandler))
	// update
	// router.Put("/page/:id", commonHandlers.Append(contentTypeHandler, bodyHandler(ping.SinglePage{})).ThenFunc(s.updatepageHandler))
	// create
	// router.Post("/page", commonHandlers.Append(contentTypeHandler, bodyHandler(ping.SinglePage{})).ThenFunc(s.createpageHandler))
	// delete
	router.Delete("/page/:id", commonHandlers.ThenFunc(s.deletepageHandler))
	// -- router.Get("/page/:id/history", commonHandlers.ThenFunc(appC.pageHistoryHandler))
	router.Options("/*name", optionsHandlers.ThenFunc(allowCorsHandler))

	// curl -X POST -H 'Accept: application/json' -H 'Content-Type: application/json' -d '{"data": {"url":"http://website.com/api", "status":0, "interval":1}}' localhost:8080/page
	l.Printf("Server started and listening on port %s. Ready for the requests.\n\n", "7171")
	if err := http.ListenAndServe(":7171", router); err != nil {
		l.Panic("Error occured: ", err)
	}
}
