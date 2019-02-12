# okpii

curl -X DELETE "localhost:9202/experts"

curl localhost:9202/experts/_search

todo list:
- deployment id parameter injection
- clear the onekeys on start
- refactor, especially the models/db


DATASTORE/INTERFACES/DEP.INJ.:
https://www.alexedwards.net/blog/organising-database-access




curl -X POST "localhost:9202/_search" -H 'Content-Type: application/json' -d'
{
  "query": {
    "bool" : {
      "filter": [
        {"term" : { "did" : 9 }},
        {"term" : { "ln" : "Sommer" }}
      ]
    }
  }
}' | json_pp