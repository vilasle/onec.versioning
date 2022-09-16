package lib

import (
	"bufio"
	"os"
	"strings"
)

func LoadEnv(path string) error {
	f, err := os.Open("dev.env")
	if err != nil {
		return err
	}

	sc := bufio.NewScanner(f)
	for sc.Scan() {
		path := strings.Split(sc.Text(), "=")
		if len(path) < 2 {
			continue
		}
		os.Setenv(path[0], path[1])
	}
	return sc.Err()
}
