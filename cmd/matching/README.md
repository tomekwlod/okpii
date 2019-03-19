## Matching command

Matching OneKey experts with the SciIQ ones. 
<br /><br />
If you run this command many times you will be creating duplicates in MySQL **_kol__onekey_** table. Simply remove the entries from MySQL first and then you run this command
<br /><br />

#### Usage example
`go run matching.go -did=1,2 -onekey=KEYHERE0123456`

##### Parameters
* `-did` [Optional] Comma sepadated list of the deployments (skip to include all of them)
* `-onekey` [Optional] If used only one key will be checked
