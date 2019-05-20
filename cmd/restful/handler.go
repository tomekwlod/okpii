package main

import (
	"net/http"
	"strconv"

	"github.com/gorilla/context"
	"github.com/julienschmidt/httprouter"
	"github.com/tomekwlod/okpii/models"
	modelsMysql "github.com/tomekwlod/okpii/models/mysql"
	strutils "github.com/tomekwlod/utils/strings"
	elastic "gopkg.in/olivere/elastic.v6"
)

// Main handlers
func (s *service) expertsHandler(w http.ResponseWriter, r *http.Request) {
	params := context.Get(r, "params").(httprouter.Params)
	did, err := strconv.Atoi(params.ByName("did"))
	if err != nil {
		s.writeError(w, &Error{"wrong_parameter", 400, "Parameter provided couldn't be used", "One of the parameters is in wrong format."}, "")
		return
	}

	type resp struct {
		Experts      int `json:"experts"`
		DeploymentID int `json:"deploymentId"`
	}

	sendResponse(w, resp{Experts: s.es.Count(did), DeploymentID: did})
}

func (s *service) pingHandler(w http.ResponseWriter, r *http.Request) {
	sendResponse(w, "OK")
}

func (s *service) dumpHandler(w http.ResponseWriter, r *http.Request) {
	params := context.Get(r, "params").(httprouter.Params)
	did, err := strconv.Atoi(params.ByName("did"))
	if err != nil {
		s.writeError(w, &Error{"wrong_parameter", 400, "Parameter provided couldn't be used", "One of the parameters is in wrong format."}, "")
		return
	}

	_, err = s.es.RemoveData(did)
	if err != nil {
		s.writeError(w, &Error{"data_error", 400, "Couldn't delete the old data", err.Error()}, "")
		return
	}

	var experts []*modelsMysql.Experts
	lastID, total := 0, 0

	for {
		// getting the experts from the MySQL
		lastID, experts, err = s.mysql.FetchExperts(lastID, did, 3000, nil)
		if err != nil {
			s.writeError(w, &Error{"data_error", 400, "Couldn't retrieve the data", err.Error()}, "")
			return
		}

		quantity := len(experts)
		total += quantity

		// stop if no results
		if quantity == 0 {
			break
		}

		// indexing the experts onto ES
		err = s.es.IndexExperts(experts, 3000)
		if err != nil {
			s.writeError(w, &Error{"index_error", 400, "Coudn't index the data", err.Error()}, "")
			return
		}
	}

	type resp struct {
		Experts      int `json:"experts"`
		DeploymentID int `json:"deploymentId"`
	}

	sendResponse(w, resp{Experts: total, DeploymentID: did})
}

// @todo: THIS NEEDS REFACTORING! IT IS JUST AN INITIAL BRIEF
func (s *service) matchHandler(w http.ResponseWriter, r *http.Request) {
	// get body from the context
	exp := context.Get(r, "body").(*models.Expert)
	result := map[int]interface{}{}

	// check the requirements
	if exp.ID == 0 || exp.Ln == "" || exp.DID == 0 {
		s.writeError(w, &Error{"wrong_parameter", 400, "Some required parameters coudn't be found", "Requirement: {id(int), ln(string), did(int)}"}, "")
		return
	}

	// check if the base expert is really the one
	k, err := s.es.FindOne(exp.ID, exp.DID, exp.Ln)
	if err != nil {
		s.writeError(w, &Error{"not_found", 400, "Expert (" + strconv.Itoa(exp.ID) + ") couldn't be found", "Synchronize the data"}, "")
		return
	}

	exclIDs := []string{strconv.Itoa(k.ID)}

	// searching
	// search query one-by-one. Cannot collect results from all of the queries because of of the merge
	// can change an expert signature so the next match may not work anymore
	// eg:
	// for an expert Xin-xia  Li we found
	//   Xin-xia  Li (3243)      <------- removed
	//   Xin      Li (909)       <--- HERE WE CHANGE THE EXPERT SIGNATURE WHICH WONT MATCH WITH THE BELOW ONE ANYMORE
	// and:
	//   X   X  Li (3243)        <------- removed
	//   Xin    Li (909)		 <--- THIS IS ACTUALLY NOT TRUE, IT IS :    Xin-xia  Li <--> X X  Li
	result, err = s.findMatches(k.Fn, k.Mn, k.Ln, "", "", k.DID, exclIDs)
	if err != nil {
		s.writeError(w, &Error{"Internal error", 404, "Error detected", err.Error()}, "")
		return
	}

	sendResponse(w, result)
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
		s.writeError(w, &Error{"not_found", 404, "Error detected", err.Error()}, "")
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
		s.writeError(w, &Error{"not_found", 404, "Error detected", err.Error()}, "")
	}

	w.Header().Set("Access-Control-Allow-Origin", "*")
	w.Header().Set("Access-Control-Allow-Headers", "Origin, X-Requested-With, Content-Type, Accept")
	w.Header().Set("Access-Control-Allow-Methods", "POST, DELETE, PUT")

	w.WriteHeader(204)
	w.Write([]byte("\n"))
}

func (s service) findMatches(fn, mn, ln, country, city string, did int, exclIDs []string) (map[int]interface{}, error) {
	result := map[int]interface{}{}

	for i := 1; i <= 5; i++ {
		switch i {

		case 1:
			m := s.es.SimpleSearch(fn, mn, ln, country, city, did, exclIDs)
			for _, row := range m {
				id := int(row["id"].(float64))
				row["type"] = "simple"
				result[id] = row
			}
			break

		case 2:
			m := s.es.ShortSearch(fn, mn, ln, country, city, did, exclIDs)
			for _, row := range m {
				id := int(row["id"].(float64))
				row["type"] = "short"
				result[id] = row
			}
			break

		case 3:
			mn0 := s.es.NoMiddleNameSearch(fn, mn, ln, country, city, did, exclIDs)
			if len(mn0) > 0 {
				// we have to check here how many other fn-mn-ln we have, if more than one we cannot merge here
				q, err := s.es.BaseQuery(did, "", exclIDs)
				if err != nil {
					return nil, err
				}

				q.Must(elastic.NewMatchPhraseQuery("ln", ln))
				q.Must(elastic.NewPrefixQuery("fn", strutils.FirstChar(fn)))
				rows, err := s.es.ExecuteQuery(q)
				if err != nil {
					return nil, err
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
			break

		case 4:
			mn1 := s.es.OneMiddleNameSearch(fn, mn, ln, country, city, did, exclIDs)
			if len(mn1) > 0 {
				// we have to check here how many other fn-mn-ln we have, if more than one we cannot merge here
				q, err := s.es.BaseQuery(did, "", exclIDs)
				if err != nil {
					return nil, err
				}

				q.Must(elastic.NewMatchPhraseQuery("ln", ln))
				q.Must(elastic.NewMatchPhraseQuery("fn", fn))
				rows, err := s.es.ExecuteQuery(q)
				if err != nil {
					return nil, err
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
			break

		case 5:
			mn2 := s.es.OneMiddleNameSearch2(fn, mn, ln, country, city, did, exclIDs)
			if len(mn2) > 0 {
				// we have to check here how many other fn-ln we have, if more than one we cannot merge here
				q, err := s.es.BaseQuery(did, "", exclIDs)
				if err != nil {
					return nil, err
				}

				q.Must(elastic.NewMatchPhraseQuery("ln", ln))
				q.Must(elastic.NewMatchPhraseQuery("fn", fn))
				rows, err := s.es.ExecuteQuery(q)
				if err != nil {
					return nil, err
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
			break

		case 6:
			r := s.es.ThreeInitialsSearch(fn, mn, ln, country, city, did, exclIDs)

			for _, row := range r {
				id := int(row["id"].(float64))
				row["type"] = "threein"
				result[id] = row
			}
			break

		default:
			break
		}

		if len(result) > 0 {
			return result, nil
		}
	}

	return nil, nil
}
