package models

// Expert model
// please keep the omitempty flag to avoid updating eg. ID=0
type Expert struct {
	ID       int    `json:"id,omitempty"`
	Fn       string `json:"fn,omitempty"`
	Mn       string `json:"mn,omitempty"`
	Ln       string `json:"ln"`
	LID      int    `json:"lid,omitempty"`
	DID      int    `json:"did,omitempty"`
	City     string `json:"city,omitempty"`
	Country  string `json:"country,omitempty"`
	Position int    `json:"position,omitempty"`
}
