## Importing experts from MySQL to Elasticsearch

This command can be safely run many times because of the index deletion on every run

#### Dependency
The script requires a mapping file named **mapping.json** located in _`data/static/mapping.json`_

#### Usage example
`go run dump.go -did=1,2 -countries=poland,germany`

##### Parameters
* `-did` [Optional] Comma separated list of the deployments (skip to include all of them)
* `-countries` [Optional] Comma separated list of the countries (skip to include all of them)
<br /><br />

#### Other

##### Manually delete the local index
`curl -X DELETE "localhost:9202/experts"`

##### Searching example:
```
curl -XGET "http://localhost:9202/experts/_search" -H 'Content-Type: application/json' -d'
{
  "query": {
    "match": {
      "country" : "GERMANY"
    }
  }
}' | json_pp
```