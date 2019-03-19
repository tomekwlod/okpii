package tools

import (
	"errors"
	"strconv"
	"strings"
)

func Deployments(str string) (deployments []string, err error) {
	// 1,2,3,9 or just 1

	deployments = strings.Split(str, ",")

	for _, d := range deployments {
		if _, err := strconv.Atoi(d); err == nil {
			continue
		}

		return nil, errors.New("One of the deploymentIDs doesn't look like a number")
	}

	if len(deployments) == 0 {
		return nil, errors.New("You need to pass at least one deployment argument to run the matching")
	}

	return
}
func Countries(str string) (countries []string, err error) {
	// Germany, Poland, switzerland

	if str != "" {
		countries = strings.Split(str, ",")
	}

	return
}
