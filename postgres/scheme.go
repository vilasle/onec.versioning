package postgres

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"regexp"

	sq "github.com/Masterminds/squirrel"
)

type Scheme []Metadata

type Metadata struct {
	FullName  string  `json:"metadata"`
	Name      string  `json:"name"`
	TableName string  `json:"table"`
	Members   []Field `json:"members"`
	VT        []VT    `json:"vt"`
}

type VT struct {
	Name    string  `json:"name"`
	Table   string  `json:"table"`
	Members []Field `json:"members"`
}

type Field struct {
	Alias string `json:"name"`
	Name  string `json:"tableMember"`
	Types []Type `json:"type"`
}

type Type struct {
	Name      string `json:"name"`
	IsSimple  bool   `json:"isSimple"`
	TableName string `json:"table"`
}

//TODO need to refactory this func, I don't like how it looks
func (s *Scheme) loadToDB() error {

	var mtId, tId, flId, tpId, vtId string

	conn, err := NewConnection(confConnection())
	if err != nil {
		return err
	}
	defer conn.Close()

	psql := sq.StatementBuilder.PlaceholderFormat(sq.Dollar)

	for _, i := range *s {
		imeta := psql.Insert("metadata").
			Columns("name,alias").
			Values(i.FullName, i.Name).
			Suffix("RETURNING \"id\"").
			RunWith(conn)

		if err := imeta.QueryRow().Scan(&mtId); err != nil {
			return err
		}

		tnum := saveOnlyNumbers(i.TableName)
		if tnum == "" {
			return fmt.Errorf("can't to get table number")
		}
		
		imt := psql.Insert("metadata_table_main").
			Columns("table_name,table_number,metadata_id").
			Values(i.TableName, tnum, mtId ).
			Suffix("RETURNING \"id\"").
			RunWith(conn)
		
		if err := imt.QueryRow().Scan(&tId); err != nil {
			return err
		}

		for _, fl := range i.Members {
			ifl := psql.Insert("field").
			Columns("field_name,alias,vt,table_id").
			Values(fl.Name, fl.Alias, false, tId).
			Suffix("RETURNING \"id\"").
			RunWith(conn)

			if err := ifl.QueryRow().Scan(&flId); err != nil {
				return err
			}
			
			for _, t := range fl.Types {
				it := psql.Insert("field_type").
				Columns("type_name,is_simple,table_name,field_id").
				Values(t.Name, t.IsSimple, t.TableName, flId).
				Suffix("RETURNING \"id\"").
				RunWith(conn)
				// tpId is useless var. Call Scan is nessasary for getting error
				if err := it.QueryRow().Scan(&tpId); err != nil {
					return err
				}
			}
		}

		for _, vt := range i.VT {
		
			vtnum := saveOnlyNumbers(vt.Table)
			if vtnum == "" {
				return fmt.Errorf("can't to get table number")
			}
			
			
			ivt := psql.Insert("metadata_table_vt").
			Columns("table_name,table_number,metadata_table_main_id").
			Values(vt.Table,vtnum, tId).
			Suffix("RETURNING \"id\"").
			RunWith(conn)

			if err := ivt.QueryRow().Scan(&vtId); err != nil {
				return err
			}

			for _, fl := range vt.Members {
				ifl := psql.Insert("field").
				Columns("field_name,alias,vt,table_id").
				Values(fl.Name, fl.Alias, true, vtId).
				Suffix("RETURNING \"id\"").
				RunWith(conn)
	
				if err := ifl.QueryRow().Scan(&flId); err != nil {
					return err
				}
				
				for _, t := range fl.Types {
					it := psql.Insert("field_type").
					Columns("type_name,is_simple,table_name,field_id").
					Values(t.Name, t.IsSimple, t.TableName, flId).
					Suffix("RETURNING \"id\"").
					RunWith(conn)
					// tpId is useless var. Call Scan is nessasary for getting error
					if err := it.QueryRow().Scan(&tpId); err != nil {
						return err
					}
				}
			}
		}
	}
	return nil
}

func saveOnlyNumbers(tn string) string {
	rs := regexp.MustCompile("[0-9]+").FindAllString(tn, -1)
	if len(rs) > 0 {
		return rs[0]
	}
	return ""
}

func cleanTables(conn *sql.DB, tbls []string) error {
	for _, t := range tbls {
		sq.Delete(t)
	}
	return nil
}

func LoadScheme(fd *os.File) error {

	sch, err := fromJson(fd)
	if err != nil {
		return err
	}

	if err := sch.loadToDB(); err != nil {
		return err
	}

	return nil
}

func fromJson(r io.Reader) (sch *Scheme, err error) {
	var bom [3]byte

	content, err := ioutil.ReadAll(r)
	if err != nil {
		return nil, err
	}

	bom[0], bom[1], bom[2] = content[0], content[1], content[2]

	if bom[0] == 0xef || bom[1] == 0xbb || bom[2] == 0xbf {
		content = content[3:]
	}

	err = json.Unmarshal(content, &sch)
	if err != nil {
		return nil, err
	}
	return
}

func confConnection() PGAuth {
	return PGAuth{
		User:     "onecversion",
		Password: "onecversion",
		Host:     "172.19.0.2",
		Port:     5432,
		SslMode:  false,
		Dbname:   "onecversion",
	}
}
