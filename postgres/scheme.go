package postgres

import (
	// "encoding/json"
	"encoding/json"
	"io"
	"io/ioutil"
	"os"
)

type Scheme struct {
	Catalogs  []Table `json:"catalog"`
	Documents []Table `json:"document"`
}

type Table struct {
	Alias      string  `json:"alias"`
	Name       string  `json:"metadata"`
	Table      string  `json:"table"`
	ChildTable bool    `json:"chilTable"`
	Fields     []Field `json:"fields"`
}

type Field struct {
	Table string `json:"table"`
	Name  string `json:"name"`
	Alias string `json:"alias"`
}

func (s *Scheme) loadToDB() error {
	return nil
}

func LoadScheme(fd *os.File) error {

	sch, err := getFromJson(fd)
	if err != nil {
		return err
	}

	if err := sch.loadToDB(); err != nil {
		return err
	}

	return nil
}

func getFromJson(r io.Reader) (sch *Scheme, err error) {
	var bom [3]byte

	content, err := ioutil.ReadAll(r)
	if err != nil {
		return nil, err
	}

	bom[0], bom[1], bom[2] = content[0], content[1], content[2]

	if bom[0] == 0xef || bom[1] == 0xbb || bom[2] == 0xbf {
		content = content[3:]
	}

	sc := Scheme{}
	err = json.Unmarshal(content, &sch)
	if err != nil {
		return nil, err
	}
	return &sc, nil
}
