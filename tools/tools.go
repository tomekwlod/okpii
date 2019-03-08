package tools

import (
	"errors"
	"os"
	"strconv"
)

func Deployments() (deployments []string, err error) {

	if len(os.Args[1:]) >= 1 {
		deployments = os.Args[1:]
	}

	if len(deployments) == 0 {
		return nil, errors.New("You need to pass at least one deployment argument to run the matching")
	}

	for _, d := range deployments {
		if _, err := strconv.Atoi(d); err == nil {
			continue
		}

		return nil, errors.New("One of the arguments doesn't look like a number")
	}

	return
}
