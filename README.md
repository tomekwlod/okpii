# okpii



##### You should run the projects in the below order:

1. Importing OneKey from CSV to MongoDB <br />
**Usage:** `make goimport`<br />
[Import doc](cmd/import/README.md)

2. Dumping SciIQ experts from MySQL to Elasticsearch <br />
**Usage:** `make godump -did=1,2,3 -countries=germany,poland`<br />
[Dump doc](cmd/dump/README.md)

3. Matching the experts <br />
**Usage:** `make gomatching -did=1,2,3 -onekey=WEM0123456789`<br />
[Matching doc](cmd/matching/README.md)
