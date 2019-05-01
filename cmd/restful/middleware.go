package main

import (
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"reflect"
	"strconv"
	"time"

	jwt "github.com/dgrijalva/jwt-go"
	tgbotapi "github.com/go-telegram-bot-api/telegram-bot-api"
	"github.com/gorilla/context"
)

var (
	errBadRequest           = &Error{"bad_request", 400, "Bad request", "Request body is not well-formed. It must be JSON."}
	errNotAcceptable        = &Error{"not_acceptable", 406, "Not Acceptable", "Accept header must be set to 'application/json'."}
	errUnsupportedMediaType = &Error{"unsupported_media_type", 415, "Unsupported Media Type", "Content-Type header must be set to: 'application/json'."}
	errInternalServer       = &Error{"internal_server_error", 500, "Internal Server Error", "Something went wrong."}
	errNotAuthorized        = &Error{"not_authorized_error", 403, "Not authorized", "Not authorized."}
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

func (s *service) writeError(w http.ResponseWriter, err *Error, loggerMessage string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(err.Status)

	if loggerMessage == "" {
		s.logger.Printf("Error received: %+v", err)
		s.logger.Printf("Returning >>%d<< status code", err.Status)
	} else {
		s.logger.Print(loggerMessage)
		s.logger.Printf("Returning >>%d<< status code", err.Status)
	}

	msg := tgbotapi.NewMessageToChannel("-1001372830179", fmt.Sprintf(`
New message from: %s
Front error: %+v
Logger message: %s`, os.Getenv("COMPOSE_PROJECT_NAME"), err, loggerMessage))

	s.telbot.Send(msg)

	json.NewEncoder(w).Encode(Errors{[]*Error{err}})
}
func sendResponse(w http.ResponseWriter, data interface{}) {
	w.Header().Set("Access-Control-Allow-Origin", "*")
	w.Header().Set("Access-Control-Allow-Headers", "Origin, X-Requested-With, Content-Type, Accept")
	w.Header().Set("Access-Control-Allow-Methods", "POST, DELETE, PUT")
	w.Header().Add("Content-Type", "application/json")

	w.WriteHeader(200)

	json.NewEncoder(w).Encode(data)
}

// recoverHandler deals with the panics
func (s *service) recoverHandler(next http.Handler) http.Handler {
	fn := func(w http.ResponseWriter, r *http.Request) {
		defer func() {
			if err := recover(); err != nil {
				s.writeError(w, errInternalServer, fmt.Sprintf("foo: %+v", err))

				botEnabled, er := strconv.ParseBool(os.Getenv("BOT_ENABLED"))
				if er != nil {
					botEnabled = false
				}
				if botEnabled {
					msg := tgbotapi.NewMessageToChannel(os.Getenv("BOT_CHANNEL"), "Panic: "+err.(string))
					s.telbot.Send(msg)
				}

				return
			}
		}()

		next.ServeHTTP(w, r)
	}

	return http.HandlerFunc(fn)
}

func (s *service) authHandler(next http.Handler) http.Handler {
	fn := func(w http.ResponseWriter, r *http.Request) {

		// https://medium.com/@adigunhammedolalekan/build-and-deploy-a-secure-rest-api-with-go-postgresql-jwt-and-gorm-6fadf3da505b
		// https://tutorialedge.net/golang/authenticating-golang-rest-api-with-jwts/

		// JWT_TOKEN has to be declared! It will be either used as an OpenToken if the JWT is disabled
		//  or it will be used as a secret within the JWT mechanism
		key := os.Getenv("JWT_TOKEN")
		if key == "" {
			s.writeError(w, errInternalServer, "No JWT Token detected in .env")
			return
		}

		jwtEnabled, err := strconv.ParseBool(os.Getenv("JWT_ENABLED"))
		if err != nil {
			jwtEnabled = false
		}

		if !jwtEnabled {

			// OPEN TOKEN WAY
			if r.Header.Get("OpenToken") != "" {
				if r.Header.Get("OpenToken") == key {
					next.ServeHTTP(w, r)
				} else {
					s.writeError(w, errNotAuthorized, "OpenToken key found but it doesn't match with the .env one")
					return
				}
			} else {
				s.writeError(w, errNotAuthorized, "No OpenToken key found in header")
				return
			}

			// JWT WAY
			// also Authorisation header needs to be filled in
		} else if jwtEnabled == true {
			if r.Header["Authorization"] != nil {

				token, err := jwt.Parse(r.Header["Authorization"][0], func(token *jwt.Token) (interface{}, error) {
					if _, ok := token.Method.(*jwt.SigningMethodHMAC); !ok {
						return nil, fmt.Errorf("There was an error")
					}
					return []byte(key), nil
				})

				if err != nil {
					s.writeError(
						w,
						&Error{"authorization_error", 500, "Authorization error", "authorization error."},
						fmt.Sprintf("JWT encryption error: %s", err.Error()))
				}

				fmt.Println(token.Claims)

				if token.Valid {
					next.ServeHTTP(w, r)
				}
			} else {
				s.writeError(w, errNotAuthorized, "Not Authorized")
				return
			}
		}
	}

	return http.HandlerFunc(fn)
}

func (s *service) loggingHandler(next http.Handler) http.Handler {
	fn := func(w http.ResponseWriter, r *http.Request) {
		s.logger.Printf("[%s] ip:%s START %q\n", r.Method, r.RemoteAddr, r.URL.String())

		t1 := time.Now()
		next.ServeHTTP(w, r)
		t2 := time.Now()

		s.logger.Printf("[%s] ip:%s DONE %q %v\n", r.Method, r.RemoteAddr, r.URL.String(), t2.Sub(t1))
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
func (s *service) contentTypeHandler(next http.Handler) http.Handler {
	fn := func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Content-Type") != "application/json" {
			s.writeError(w, errUnsupportedMediaType, "")
			return
		}

		next.ServeHTTP(w, r)
	}

	return http.HandlerFunc(fn)
}

func (s *service) bodyHandler(v interface{}) func(http.Handler) http.Handler {
	t := reflect.TypeOf(v)

	m := func(next http.Handler) http.Handler {
		fn := func(w http.ResponseWriter, r *http.Request) {
			val := reflect.New(t).Interface()

			err := json.NewDecoder(r.Body).Decode(val)
			if err != nil {
				s.writeError(w, errBadRequest, "")
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
