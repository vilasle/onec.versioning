package raw

import (
	"regexp"
	"strings"
)


func CheckRawData(raw string) (prepdata string, ok bool) {
	t := regexp.MustCompile(`[\d]{1,6}:[[:xdigit:]]{32}`)
	prepdata = t.FindString(raw)

	ok = prepdata != ""

	return
}

func GetMainColumsAsVars(predata string) (string, string) {
	parts := strings.Split(predata, ":")
	return parts[0], parts[1]
}