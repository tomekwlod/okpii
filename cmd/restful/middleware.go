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

func writeError(w http.ResponseWriter, err *Error) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(err.Status)

	json.NewEncoder(w).Encode(Errors{[]*Error{err}})
}
func sendResponse(w http.ResponseWriter, data interface{}) {
	w.Header().Set("Access-Control-Allow-Origin", "*")
	w.Header().Set("Access-Control-Allow-Headers", "Origin, X-Requested-With, Content-Type, Accept")
	w.Header().Set("Access-Control-Allow-Methods", "POST, DELETE, PUT")
	w.Header().Add("Content-Type", "application/json")

	json.NewEncoder(w).Encode(data)
}

// recoverHandler deals with the panics
func (s *service) recoverHandler(next http.Handler) http.Handler {
	fn := func(w http.ResponseWriter, r *http.Request) {
		defer func() {
			if err := recover(); err != nil {
				s.logger.Printf("panic: %+v", err)
				writeError(w, errInternalServer)
				return
			}
		}()

		next.ServeHTTP(w, r)
	}

	return http.HandlerFunc(fn)
}

func (s *service) authHandler(next http.Handler) http.Handler {
	fn := func(w http.ResponseWriter, r *http.Request) {

		// JWT_TOKEN has to be declared! It will be either used as an OpenToken if the JWT is disabled
		//  or it will be used as a secret within the JWT mechanism
		key := os.Getenv("JWT_TOKEN")
		if key == "" {
			s.logger.Print("No JWT Token detected in .env")
			writeError(w, errInternalServer)
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
					s.logger.Print("OpenToken key found but it doesn't match with the .env one")
					writeError(w, errNotAuthorized)
					return
				}
			} else {
				s.logger.Print("No OpenToken key found in header")
				writeError(w, errNotAuthorized)
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
					s.logger.Printf("JWT encryption error: %s", err.Error())
					writeError(w, &Error{"authorization_error", 500, "Authorization error", "authorization error."})
				}

				fmt.Println(token.Claims)

				if token.Valid {
					next.ServeHTTP(w, r)
				}
			} else {
				s.logger.Print("Not Authorized")
				writeError(w, errNotAuthorized)
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
func contentTypeHandler(next http.Handler) http.Handler {
	fn := func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Content-Type") != "application/json" {
			writeError(w, errUnsupportedMediaType)
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
				writeError(w, errBadRequest)
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
