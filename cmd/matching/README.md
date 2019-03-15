-- Matching command

Parameters:
-did     Comma sepadated list of the deployments; Skip to include all of them
-onekey  To test only one key

Usage:
go run matching.go -did=1,2 -onekey=KEYHERE0123456


If you run this command many times you will be creating duplicates in mysql. Simply remove the entries from the mysql before you run this command