package raw

import (
	"regexp"
	"strings"
)

func CheckRawDataUserAndRef(raw string) (predata string, ok bool) {
	t := regexp.MustCompile(`[\d]{1,6}:[[:xdigit:]]{32}`)
	predata = t.FindString(raw)

	ok = predata != ""

	return
}

func CheckRawDataRef(raw string) bool {
	return regexp.MustCompile(`^{"#",+[[:xdigit:]]{8}(-[[:xdigit:]]{4}){3}-[[:xdigit:]]{12},[\d]{1,6}:[[:xdigit:]]{32}}$`).MatchString(raw)
}

func GetMainColumsAsVars(predata string) (string, string) {
	parts := strings.Split(predata, ":")
	return parts[0], parts[1]
}
