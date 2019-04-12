## Matching command

#### Currently it is just a concept
<br />
The plan is to use the dump command to move the data from MySQL to ES and then use only this data to clean it up. It would run agains each other (within a deployment) and merge the matches using models/repo/*searches
<br /><br />
The challenge will be to interact it with the current PHP project and run it on a specific command request. Command should wait for it to finish the merging fully. 
<br />
