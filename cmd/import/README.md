## Importing data from csv to mongodb

The script imports the data from the CSV file provided by the OneKy to a MongoDB instance. Before you run the import please double check the column standards below and change them if required!


SRC_CUST_ID | CUST_NAME | FIRST_NAME | LAST_NAME | CITY | CNTRY
---|----|---|---|---|---
WDEM01384440 | MARY BOROWA | MARY | BOROWA | HALSBRÜCKE| GERMANY
WDEM00555739 | LINA SCHMITZ | LINA | SCHMITZ | HEINSBERG | GERMANY
WDEM00555720 | URLICH DARMOUL | URLICH | DARMOUL | SENFTENBERG |GERMANY

The order of the columns doesn't matter but the column naming matters! The above example contains all needed columns and it's a MUST! There can be more columns attached but they will be simply ignored by the script.

##WARNING!
##### REMEMBER TO NORMALIZE THE COUNTRIES IN THE CSV FILE TO FOLLOW THE COUNTRIES IN THE MYSQL DATABASE (_location.country_name_ table)! FOR EXAMPLE CHANGE 'DEU' TO 'GERMANY', 'PL' TO 'POLAND', ETC

### TODO
* drop the data in mongo first
* change the docker execution method