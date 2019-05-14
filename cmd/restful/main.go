package main

import (
	"io"
	"log"
	"net/http"
	"os"
	"strconv"

	_ "github.com/go-sql-driver/mysql"
	tgbotapi "github.com/go-telegram-bot-api/telegram-bot-api"
	"github.com/gorilla/context"
	"github.com/justinas/alice"
	"github.com/tomekwlod/okpii/models"
	modelsES "github.com/tomekwlod/okpii/models/es"
	modelsMysql "github.com/tomekwlod/okpii/models/mysql"
	ml "github.com/tomekwlod/utils/logger"
)

// service struct to hold the db and the logger
type service struct {
	es     modelsES.Repository
	mysql  modelsMysql.Repository
	logger *ml.Logger
	telbot *tgbotapi.BotAPI
}

func main() {

	// definig the logger & a log file
	file, err := os.OpenFile("log/http.log", os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0666)
	if err != nil {
		log.Fatalln("Failed to open log file", err)
	}
	multi := io.MultiWriter(file, os.Stdout)
	l := ml.New(
		os.Getenv("LOGGING_MODE"),
		log.New(multi, "", log.Ldate|log.Ltime|log.Lshortfile),
	)

	esClient, err := modelsES.ESClient()
	if err != nil {
		log.Fatalln("Failed to connect to ES", err)
	}

	mysqlClient, err := modelsMysql.MysqlClient()
	if err != nil {
		log.Fatalln("Failed to connect to MySQL", err)
	}
	defer mysqlClient.Close()

	bot, err := tgbotapi.NewBotAPI(os.Getenv("BOT_TOKEN"))
	if err != nil {
		log.Fatalln("Failed to establish the Telegram connection")
	}
	botDebug, err := strconv.ParseBool(os.Getenv("BOT_DEBUG"))
	if err != nil {
		botDebug = false
	}
	bot.Debug = botDebug

	s := &service{
		es:     esClient,
		mysql:  mysqlClient,
		logger: l,
		telbot: bot,
	}

	commonHandlers := alice.New(
		context.ClearHandler, // ClearHandler wraps an http.Handler and clears request values at the end of a request lifetime
		s.loggingHandler,     // displaying logs in a consistent way
		s.recoverHandler,     // deals with the panic-s
		s.authHandler,        // checking the auth
		acceptHandler,        // accepts only requests types we want
	)
	optionsHandlers := alice.New(context.ClearHandler, s.loggingHandler, s.recoverHandler)
	pingHandlers := alice.New(context.ClearHandler, s.recoverHandler)

	router := newRouter()

	// dump experts for one deployment - takes time
	router.Get(
		"/__ping",
		pingHandlers.ThenFunc(s.pingHandler))

	// dump experts for one deployment - takes time
	router.Get(
		"/dump/:did",
		commonHandlers.ThenFunc(s.dumpHandler))

	// counting experts for one deployment
	router.Get(
		"/experts/:did",
		commonHandlers.ThenFunc(s.expertsHandler))

	// marks expert as deleted
	router.Delete(
		"/expert/:id",
		commonHandlers.ThenFunc(s.deleteHandler))

	// finding a match for a given expert details
	router.Post(
		"/match",
		commonHandlers.Append(
			s.contentTypeHandler,
			s.bodyHandler(models.Expert{}),
		).ThenFunc(s.matchHandler))

	// update expert's details
	router.Put(
		"/expert/:id",
		commonHandlers.Append(
			s.contentTypeHandler,
			s.bodyHandler(models.Expert{}),
		).ThenFunc(s.updateHandler))

	// CORS support
	router.Options(
		"/*name",
		optionsHandlers.ThenFunc(allowCorsHandler))

	port := "7171"
	if os.Getenv("WEB_PORT") != "" {
		port = os.Getenv("WEB_PORT")
	}

	l.Printf("\n\n----------------------\nListening on port %s\n\n", port)
	if err := http.ListenAndServe(":"+port, router); err != nil {
		l.Panic("Error occured: ", err)
	}
}
