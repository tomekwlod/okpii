-- Importing experts from MySQL to Elasticsearch

Parameters:
-did        Comma sepadated list of the deployments; Skip to include all of them
-countries  Comma separated list of the countries; Skip to inclide all of them

Usage:
go run dump.go -did=1,2 -countries=poland,germany


This command can be run many times with the same parameters because all the duplicated in ES will be re-indexed

If you want to remove the index anyway run the below:
curl -X DELETE "localhost:9202/experts"