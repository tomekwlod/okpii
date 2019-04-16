package main

import (
	"encoding/json"
	"net/http"
	"reflect"
	"strconv"
	"time"

	"github.com/gorilla/context"
	"github.com/julienschmidt/httprouter"
	"github.com/tomekwlod/okpii/models"
	strutils "github.com/tomekwlod/utils/strings"
	elastic "gopkg.in/olivere/elastic.v6"
)

var (
	errBadRequest           = &Error{"bad_request", 400, "Bad request", "Request body is not well-formed. It must be JSON."}
	errNotAcceptable        = &Error{"not_acceptable", 406, "Not Acceptable", "Accept header must be set to 'application/json'."}
	errUnsupportedMediaType = &Error{"unsupported_media_type", 415, "Unsupported Media Type", "Content-Type header must be set to: 'application/json'."}
	errInternalServer       = &Error{"internal_server_error", 500, "Internal Server Error", "Something went wrong."}
)

// Errors
type Errors struct {
	Errors []*Error `json:"errors"`
}
type Error struct {
	Id     string `json:"id"`
	Status int    `json:"status"`
	Title  string `json:"title"`
	Detail string `json:"detail"`
}

func WriteError(w http.ResponseWriter, err *Error) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(err.Status)

	json.NewEncoder(w).Encode(Errors{[]*Error{err}})
}

func wrapHandler(h http.Handler) httprouter.Handle {
	return func(w http.ResponseWriter, r *http.Request, ps httprouter.Params) {
		context.Set(r, "params", ps)
		h.ServeHTTP(w, r)
	}
}

// Middlewares
func recoverHandler(next http.Handler) http.Handler {
	fn := func(w http.ResponseWriter, r *http.Request) {
		defer func() {
			if err := recover(); err != nil {
				// s.logger.Printf("panic: %+v", err)
				WriteError(w, errInternalServer)
				return
			}
		}()

		next.ServeHTTP(w, r)
	}

	return http.HandlerFunc(fn)
}

func (s *service) loggingHandler(next http.Handler) http.Handler {
	fn := func(w http.ResponseWriter, r *http.Request) {
		t1 := time.Now()
		next.ServeHTTP(w, r)
		t2 := time.Now()

		s.logger.Printf("[%s] %q %v\n", r.Method, r.URL.String(), t2.Sub(t1))
		// log.Printf("[%s] %q %v\n", r.Method, r.URL.String(), t2.Sub(t1))
	}

	return http.HandlerFunc(fn)
}

// Here is my request and I would like (to Accept) this response format
// I expect to receive this format only
func acceptHandler(next http.Handler) http.Handler {
	fn := func(w http.ResponseWriter, r *http.Request) {
		// if r.Header.Get("Accept") != "application/json" {
		// 	WriteError(w, errNotAcceptable)
		// 	return
		// }

		next.ServeHTTP(w, r)
	}

	return http.HandlerFunc(fn)
}

// Content-Type header tells the server what the attached data actually is
// Only for PUT & POST
func contentTypeHandler(next http.Handler) http.Handler {
	fn := func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Content-Type") != "application/json" {
			WriteError(w, errUnsupportedMediaType)
			return
		}

		next.ServeHTTP(w, r)
	}

	return http.HandlerFunc(fn)
}

func bodyHandler(v interface{}) func(http.Handler) http.Handler {
	t := reflect.TypeOf(v)

	m := func(next http.Handler) http.Handler {
		fn := func(w http.ResponseWriter, r *http.Request) {
			val := reflect.New(t).Interface()

			err := json.NewDecoder(r.Body).Decode(val)
			if err != nil {
				WriteError(w, errBadRequest)
				return
			}

			if next != nil {
				context.Set(r, "body", val)
				next.ServeHTTP(w, r)
			}
		}

		return http.HandlerFunc(fn)
	}

	return m
}

// allow CORS
func allowCorsHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method == "OPTIONS" {
		w.Header().Set("Access-Control-Allow-Origin", "*")
		w.Header().Set("Access-Control-Allow-Headers", "Origin, X-Requested-With, Content-Type, Accept")
		w.Header().Set("Access-Control-Allow-Methods", "POST, DELETE, PUT")

		w.WriteHeader(200)
	}
}

// Main handlers
func (s *service) expertsHandler(w http.ResponseWriter, r *http.Request) {
	params := context.Get(r, "params").(httprouter.Params)
	did, err := strconv.Atoi(params.ByName("did"))
	if err != nil {
		WriteError(w, &Error{"wrong_parameter", 400, "Parameter provided couldn't be used", "One of the parameters is in wrong format."})
		return
	}

	count := s.es.Count(did)

	w.Header().Set("Access-Control-Allow-Origin", "*")
	w.Header().Set("Access-Control-Allow-Headers", "Origin, X-Requested-With, Content-Type, Accept")
	w.Header().Set("Access-Control-Allow-Methods", "POST, DELETE, PUT")
	w.Header().Set("Content-Type", "application/json")

	type resp struct {
		Experts      int `json:"experts"`
		DeploymentID int `json:"deploymentId"`
	}
	json.NewEncoder(w).Encode(resp{Experts: count, DeploymentID: did})
}

// @todo: THIS NEEDS REFACTORING! IT IS JUST AN INITIAL BRIEF
func (s *service) matchHandler(w http.ResponseWriter, r *http.Request) {
	// get body from the context
	exp := context.Get(r, "body").(*models.Expert)
	result := map[int]interface{}{}

	// check the requirements
	if exp.ID == 0 || exp.Ln == "" || exp.DID == 0 {
		WriteError(w, &Error{"wrong_parameter", 400, "Some required parameters coudn't be found", "Requirement: {id(int), ln(string), did(int)}"})
		return
	}

	// check if the base expert is really the one
	k, err := s.es.FindOne(exp.ID, exp.DID, exp.Ln)
	if err != nil {
		WriteError(w, &Error{"not_found", 400, "Expert (" + strconv.Itoa(exp.ID) + ") couldn't be found", "Synchronize the data"})
		return
	}

	exclIDs := []string{strconv.Itoa(k.ID)}
	// searching
	m := s.es.SimpleSearch(k.Fn, k.Mn, k.Ln, "", "", k.DID, exclIDs)
	for _, row := range m {
		id := int(row["id"].(float64))
		row["type"] = "simple"
		result[id] = row
		// result = append(result, strconv.FormatFloat(row["id"].(float64), 'f', 0, 64))
	}
	m = s.es.ShortSearch(k.Fn, k.Mn, k.Ln, "", "", k.DID, exclIDs)
	for _, row := range m {
		id := int(row["id"].(float64))
		row["type"] = "short"
		result[id] = row
	}

	mn0 := s.es.NoMiddleNameSearch(k.Fn, k.Mn, k.Ln, "", "", k.DID, exclIDs)
	if len(mn0) > 0 {
		// we have to check here how many other fn-mn-ln we have, if more than one we cannot merge here
		q, err := s.es.BaseQuery(k.DID, "", exclIDs)
		if err != nil {
			s.logger.Println("Error detected", err)
			WriteError(w, &Error{"Internal error", 404, "Error detected", err.Error()})
			return
		}

		q.Must(elastic.NewMatchPhraseQuery("ln", k.Ln))
		q.Must(elastic.NewPrefixQuery("fn", strutils.FirstChar(k.Fn)))
		rows, err := s.es.ExecuteQuery(q)
		if err != nil {
			s.logger.Println("Error detected", err)
			WriteError(w, &Error{"not_found", 404, "Error detected", err.Error()})
			return
		}

		if len(rows) == 1 {
			for _, row := range rows {
				id := int(row["id"].(float64))
				row["type"] = "nomid"
				result[id] = row
			}
		} else {
			s.logger.Println("There is more people with the same initials Fn% Ln")
		}
	}

	mn1 := s.es.OneMiddleNameSearch(k.Fn, k.Mn, k.Ln, "", "", k.DID, exclIDs)
	if len(mn1) > 0 {
		// we have to check here how many other fn-mn-ln we have, if more than one we cannot merge here
		q, err := s.es.BaseQuery(k.DID, "", nil)
		if err != nil {
			s.logger.Println("Error detected", err)
			WriteError(w, &Error{"Internal error", 404, "Error detected", err.Error()})
			return
		}

		q.Must(elastic.NewMatchPhraseQuery("ln", k.Ln))
		q.Must(elastic.NewMatchPhraseQuery("fn", k.Fn))
		rows, err := s.es.ExecuteQuery(q)
		if err != nil {
			s.logger.Println("Error detected", err)
			WriteError(w, &Error{"Internal error", 404, "Error detected", err.Error()})
			return
		}

		if len(rows) == 1 {
			for _, row := range rows {
				id := int(row["id"].(float64))
				row["type"] = "onemid1"
				result[id] = row
			}
		} else {
			s.logger.Println("There is more people with the same initials Fn *Mn* Ln")
		}
	}

	mn2 := s.es.OneMiddleNameSearch2(k.Fn, k.Mn, k.Ln, "", "", k.DID, exclIDs)
	if len(mn2) > 0 {
		// we have to check here how many other fn-ln we have, if more than one we cannot merge here
		q, err := s.es.BaseQuery(k.DID, "", exclIDs)
		if err != nil {
			s.logger.Println("Error detected", err)
			WriteError(w, &Error{"Internal error", 404, "Error detected", err.Error()})
			return
		}

		q.Must(elastic.NewMatchPhraseQuery("ln", k.Ln))
		q.Must(elastic.NewMatchPhraseQuery("fn", k.Fn))
		rows, err := s.es.ExecuteQuery(q)
		if err != nil {
			s.logger.Println("Error detected", err)
			WriteError(w, &Error{"Internal error", 404, "Error detected", err.Error()})
			return
		}

		if len(rows) == 1 {
			for _, row := range rows {
				id := int(row["id"].(float64))
				row["type"] = "onemid2"
				result[id] = row
			}
		} else {
			s.logger.Println("There is more people with the same initials Fn1% Ln")
		}
	}

	s.logger.Println(result)

	w.Header().Set("Access-Control-Allow-Origin", "*")
	w.Header().Set("Access-Control-Allow-Headers", "Origin, X-Requested-With, Content-Type, Accept")
	w.Header().Set("Access-Control-Allow-Methods", "POST, DELETE, PUT")
	w.Header().Set("Content-Type", "application/json")

	w.WriteHeader(200)
	json.NewEncoder(w).Encode(result)
}

func (s *service) updateHandler(w http.ResponseWriter, r *http.Request) {
	params := context.Get(r, "params").(httprouter.Params)
	id := params.ByName("id")
	body := context.Get(r, "body").(*models.Expert)

	if id == "" {
		s.logger.Panicln("ID cannot be empty")
	}

	err := s.es.UpdatePartially(id, *body)
	if err != nil {
		s.logger.Println("Error detected", err)
		WriteError(w, &Error{"not_found", 404, "Error detected", err.Error()})
	}

	w.Header().Set("Access-Control-Allow-Origin", "*")
	w.Header().Set("Access-Control-Allow-Headers", "Origin, X-Requested-With, Content-Type, Accept")
	w.Header().Set("Access-Control-Allow-Methods", "POST, DELETE, PUT")

	w.WriteHeader(204)
	w.Write([]byte("\n"))
}

func (s *service) deleteHandler(w http.ResponseWriter, r *http.Request) {
	params := context.Get(r, "params").(httprouter.Params)
	id := params.ByName("id")

	if id == "" {
		s.logger.Panicln("ID cannot be empty")
	}

	err := s.es.MarkAsDeleted(id)
	if err != nil {
		s.logger.Println("Error detected", err)
		WriteError(w, &Error{"not_found", 404, "Error detected", err.Error()})
	}

	w.Header().Set("Access-Control-Allow-Origin", "*")
	w.Header().Set("Access-Control-Allow-Headers", "Origin, X-Requested-With, Content-Type, Accept")
	w.Header().Set("Access-Control-Allow-Methods", "POST, DELETE, PUT")

	w.WriteHeader(204)
	w.Write([]byte("\n"))
}
