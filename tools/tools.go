package tools

import (
	"errors"
	"os"
	"strconv"
	"strings"
)

func Deployments() (deployments []string, err error) {
	// 1,2,3,9 or just 1

	if len(os.Args[1:]) >= 1 {
		deployments = strings.Split(os.Args[1], ",")

		for _, d := range deployments {
			if _, err := strconv.Atoi(d); err == nil {
				continue
			}

			return nil, errors.New("One of the arguments doesn't look like a number")
		}
	}

	if len(deployments) == 0 {
		return nil, errors.New("You need to pass at least one deployment argument to run the matching")
	}

	return
}

func OKTest() (oneky string) {

	if len(os.Args[2:]) >= 1 {
		oneky = os.Args[2]
	}

	return
}
