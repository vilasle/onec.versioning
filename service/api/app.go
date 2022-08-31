package main

import (
	"bufio"
	"database/sql"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"regexp"
	"strconv"
	"strings"

	sq "github.com/Masterminds/squirrel"
	"github.com/go-chi/chi/v5"
	"github.com/go-chi/chi/v5/middleware"
	"github.com/vilamslep/onec.versioning/dbms"
	"github.com/vilamslep/onec.versioning/entity"
	"github.com/vilamslep/onec.versioning/lib"
	"github.com/vilamslep/onec.versioning/logger"
)

type Records []Record

type Record struct {
	Id            string `json:"id"`
	OnecTimestamp string `json:"data"`
	User          string `json:"user"`
	Number        string `json:"number"`
}

var (
	pg *sql.DB
	ms *sql.DB
)

func dbg_LoadEnv() error {
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

var (
	boolTrue  = "0x01"
	boolFalse = "0x00"
	fldNumber = "НомерСтроки"

	fldTypeBool     = "0x02"
	fldSuffixBool   = "_L"
	fldTypeInt      = "0x03"
	fldSuffixInt    = "_N"
	fldTypeDate     = "0x04"
	fldSuffixDate   = "_T"
	fldTypeString   = "0x05"
	fldSuffixString = "_S"
	fldTypeRef      = "0x08"
	fldRefNumber    = "_RRRef"
	fldSuffixRef    = "_RRRef"
	vlRefEmpty      = "0x00000000000000000000000000000000"
)

func main() {

	dbg_LoadEnv()

	logger.Info("Start web service")

	var err error
	pg, ms, err = lib.CreateConnections()
	if err != nil {
		logger.Fatal(err)
	}

	r := chi.NewRouter()
	r.Use(middleware.Logger)
	r.Use(middleware.Recoverer)
	r.Use(middleware.RequestID)

	r.Route("/version", func(r chi.Router) {
		r.With()
		r.Get("/{table}/list", List)
		r.Get("/{table}/entity", EntityById)
	})

	http.ListenAndServe(":3056", r)
}

func List(w http.ResponseWriter, r *http.Request) {

	tabn := chi.URLParam(r, "table")

	ref := r.URL.Query().Get("ref")
	if ref == "" {
		w.WriteHeader(http.StatusBadRequest)
		w.Write([]byte("required parameters is not defined"))
	}

	psql := sq.StatementBuilder.PlaceholderFormat(sq.Dollar)

	tab := "version_" + tabn
	sl := psql.Select("id,user_id,to_char(version_timestamp,'YYYYMMDDHH24MISS') as version_date,version_number").
		From(tab).
		Where(sq.Eq{"ref": ref}).
		RunWith(pg)

	rows, err := sl.Query()
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		w.Write([]byte(err.Error()))
		return
	}

	data := make(Records, 0)
	for rows.Next() {
		var id, user, timestamp, number string

		if err := rows.Scan(&id, &user, &timestamp, &number); err != nil {
			w.WriteHeader(http.StatusInternalServerError)
			w.Write([]byte(err.Error()))
			return
		}
		data = append(data, Record{
			OnecTimestamp: timestamp,
			User:          user,
			Number:        number,
			Id:            id,
		})
	}

	if content, err := json.Marshal(data); err == nil {
		w.Write(content)
	} else {
		w.WriteHeader(http.StatusInternalServerError)
		w.Write([]byte(err.Error()))
	}
}

func EntityById(w http.ResponseWriter, r *http.Request) {

	var content, ref string

	id := r.URL.Query().Get("id")
	tabn := chi.URLParam(r, "table")

	tab := "version_" + tabn

	psql := sq.StatementBuilder.PlaceholderFormat(sq.Dollar)
	sl := psql.Select("content,ref").
		From(tab).
		Where(sq.Eq{"id": id}).
		RunWith(pg)

	rows, err := sl.Query()
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		w.Write([]byte(err.Error()))
		return
	}

	row, err := dbms.ReadRow(rows)
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		w.Write([]byte(err.Error()))
		return
	}

	if len(row) == 0 {
		w.Write([]byte("empty"))
		return
	}

	content = row["content"]
	ref = row["ref"]

	ver := entity.Version{}
	if err := json.Unmarshal([]byte(content), &ver); err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		w.Write([]byte(err.Error()))
		return
	}
	_, err = generateSqlTxt(ver, tabn, ref)
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		w.Write([]byte(err.Error()))
	}

	_, err = generateSqlTxtVt(ver, ref)
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		w.Write([]byte(err.Error()))
	}
	w.Write([]byte("success"))
}

func generateSqlTxt(ver entity.Version, tabnum string, ref string) (map[string]string, error) {
	psql := sq.StatementBuilder.PlaceholderFormat(sq.Dollar)
	sfl := psql.Select(`
			t1.table_name as mainName, t2.field_name,
			t2.alias, t3.type_name, t3.table_name,
			CASE WHEN t5.alias IS NULL THEN '' ELSE t5.alias END as alias_table`).
		From("metadata_table_main AS t1").
		LeftJoin("field AS t2 ON t1.id = t2.table_id_main").
		LeftJoin("field_type AS t3 ON t2.id = t3.field_id").
		LeftJoin("metadata_table_main AS t4 ON t3.table_name = t4.table_name").
		LeftJoin("metadata as t5 ON t4.metadata_id = t5.id").
		Where(sq.Eq{"t1.table_number": tabnum}).
		RunWith(pg)

	flrows, err := sfl.Query()
	if err != nil {
		return nil, err
	}
	defer flrows.Close()

	var tabName, fldName, alias, fldType, fldTable, fldTableAlias string
	flds := make(map[string]map[string]string, 0)
	for flrows.Next() {
		if err := flrows.Scan(&tabName, &fldName, &alias, &fldType, &fldTable, &fldTableAlias); err != nil {
			return nil, err
		}
		flds[fldName] = map[string]string{
			"alias":       alias,
			"type":        fldType,
			"table":       fldTable,
			"table_alias": fldTableAlias,
		}
	}

	selector := make([]string, 0, len(flds))
	for k, i := range flds {
		typeName := i["type"]
		switch typeName {
		case "ref":
			selector = append(selector, fmt.Sprintf("CONVERT(VARCHAR(34), t1.%s, 1) AS %s", k, k))
		case "bool":
			selector = append(selector, fmt.Sprintf("CONVERT(VARCHAR(4),t1.%s,1) AS %s", k, k))
		case "string":
			selector = append(selector, fmt.Sprintf("t1.%s AS %s", k, k))
		case "int":
			selector = append(selector, fmt.Sprintf("t1.%s AS %s", k, k))
		case "date":
			selector = append(selector, fmt.Sprintf("FORMAT(t1.%s, 'dd.MM.yyyy HH:mm:ss', 'ru-RU') AS %s", k, k))
		default:
			return nil, fmt.Errorf("Unsupported type of field, %s %s", k, typeName)
		}
	}
	sel := sq.Select(strings.Join(selector, ",")).From("dbo." + tabName + " AS t1").Where(sq.Eq{"CONVERT(VARCHAR(34), t1._IDRRef, 2)": ref}).RunWith(ms)

	rows, err := sel.Query()

	if err != nil {
		return nil, err
	}
	res, err := dbms.ReadRow(rows)
	if err != nil {
		return nil, err
	}
	arJoin := make([]string, 0, len(res))
	selector = make([]string, 0, len(res))
	tableCounter := 2
	enums := make(map[string]string)
	for k, v := range res {
		sets := flds[k]
		tabl := sets["table"]
		typeName := sets["type"]
		tablAlias := sets["table_alias"]

		switch typeName {
		case "ref":
			if v == vlRefEmpty {
				selector = append(selector, fmt.Sprintf("'' AS %s", k))
				continue
			}

			if is, flnum := isCompositeField(k, res); is {
				fldT := fmt.Sprintf("_Fld%s_TYPE", flnum)
				fldRT := fmt.Sprintf("_Fld%s_RTRef", flnum)
				fldRef := fmt.Sprintf("_Fld%s_RRRef", flnum)
				refV := res[fldRef]

				if refV == vlRefEmpty {
					selector = append(selector, fmt.Sprintf("'' AS %s", k))
					continue
				}

				typeField := res[fldT]
				switch typeField {
				case fldTypeBool:
					fld := fmt.Sprintf("_Fld%s_L", flnum)
					selector = append(selector, fmt.Sprintf("CASE WHEN t1.%s = 0x00 THEN 0 ELSE 1 END AS %s", fld, k))
				case fldTypeInt:
					fld := fmt.Sprintf("_Fld%s_N", flnum)
					selector = append(selector, fmt.Sprintf("t1.%s AS %s", fld, k))
				case fldTypeDate:
					fld := fmt.Sprintf("_Fld%s_", flnum)
					selector = append(selector, fmt.Sprintf("FORMAT(t1.%s, 'dd.MM.yyyy HH:mm:ss', 'ru-RU') AS %s", fld, fldRef))
				case fldTypeString:
					fld := fmt.Sprintf("_Fld%s_S", flnum)
					selector = append(selector, fmt.Sprintf("t1.%s AS %s", fld, fldRef))
				case fldTypeRef:
					valRT := res[fldRT]
					numTab, err := strconv.ParseInt(valRT[2:], 16, 64)
					if err != nil {
						return nil, err
					}
					psql := sq.StatementBuilder.PlaceholderFormat(sq.Dollar)
					sl := psql.Select("table_name").From("metadata_table_main").Where(sq.Eq{"table_number": numTab}).RunWith(pg)
					var tableName string
					if err := sl.QueryRow().Scan(&tableName); err != nil {
						return nil, err
					}
					arJoin = append(arJoin,
						fmt.Sprintf("dbo.%s AS t%d ON t1.%s = t%d._IDRRef", tableName, tableCounter, fldRef, tableCounter))

					if strings.Contains(tableName, "Document") {
						selector = append(selector, fmt.Sprintf(
							"CONCAT('%s № ', t%d._Number, ' от ',   FORMAT(t%d._Date_Time, 'dd.MM.yyyy HH:mm:ss', 'ru-RU')) AS %s",
							tablAlias, tableCounter, tableCounter, k))
					} else if strings.Contains(tableName, "Reference") {
						selector = append(selector, fmt.Sprintf("t%d._Description AS %s", tableCounter, fldRef))
					} else if strings.Contains(tableName, "Enum") {
						selector = append(selector, fmt.Sprintf("t%d._EnumOrder AS %s", tableCounter, fldRef))
						enums[fldRef] = tableName
					}

					tableCounter++
				}
				delete(res, fldT)
				delete(res, fldRef)
				delete(res, fldRT)
			} else {
				arJoin = append(arJoin,
					fmt.Sprintf("dbo.%s AS t%d ON t1.%s = t%d._IDRRef", tabl, tableCounter, k, tableCounter))

				if strings.Contains(tabl, "Document") {
					selector = append(selector, fmt.Sprintf(
						"CONCAT('%s № ', t%d._Number, ' от ',   FORMAT(t%d._Date_Time, 'dd.MM.yyyy HH:mm:ss', 'ru-RU')) AS %s",
						tablAlias, tableCounter, tableCounter, k))
				} else if strings.Contains(tabl, "Reference") {
					selector = append(selector, fmt.Sprintf("t%d._Description AS %s", tableCounter, k))
				} else if strings.Contains(tabl, "Enum") {
					selector = append(selector, fmt.Sprintf("t%d._EnumOrder AS %s", tableCounter, k))
					enums[k] = tabl
				}
				tableCounter++
			}
		case "bool":
			selector = append(selector, fmt.Sprintf("CASE WHEN t1.%s = 0x00 THEN 0 ELSE 1 END AS %s", k, k))
		case "string":
			selector = append(selector, fmt.Sprintf("t1.%s AS %s", k, k))
		case "int":
			selector = append(selector, fmt.Sprintf("t1.%s AS %s", k, k))
		case "date":
			selector = append(selector, fmt.Sprintf("FORMAT(t1.%s, 'dd.MM.yyyy HH:mm:ss', 'ru-RU') AS %s", k, k))
		default:
			return nil, fmt.Errorf("Unsupported type of field, %s %s", k, typeName)
		}
	}

	base := sq.Select(strings.Join(selector, ",")).From("dbo." + tabName + " AS t1")
	for _, tj := range arJoin {
		base = base.LeftJoin(tj)
	}
	base = base.Where(sq.Eq{"CONVERT(VARCHAR(34), t1._IDRRef, 2)": ref}).RunWith(ms)

	row, err := base.Query()
	if err != nil {
		return nil, err
	}
	record, err := dbms.ReadRow(row)
	if err != nil {
		return nil, err
	}
	if len(enums) > 0 {
		for k, v := range enums {
			slen := psql.Select("t2.alias").
				From("enums AS t1").
				LeftJoin("enums_value AS t2 ON t1.id = t2.enum_id").
				Where(sq.Eq{"t1.enum_name": v, "t2.order_enum": record[k]}).
				RunWith(pg)

			var enumAlias string
			if err := slen.QueryRow().Scan(&enumAlias); err != nil {
				return nil, err
			}
			record[k] = enumAlias
		}
	}
	prettyRecord := make(map[string]string)
	for k, v := range flds {
		if val, ok := record[k]; ok {
			prettyRecord[v["alias"]] = val
		} else {
			if strings.Contains(k, "_TYPE") && strings.Contains(k, "_RTRef") {
				continue
			}
		}

	}
	return prettyRecord, err
}

func generateSqlTxtVt(ver entity.Version, ref string) (map[string][]map[string]string, error) {
	vts := make(map[string][]map[string]string,0)
	for chilTable, v := range ver.VT {
		if len(v) == 0 {
			vts[chilTable] = make([]map[string]string, 0)
			continue
		}
		psql := sq.StatementBuilder.PlaceholderFormat(sq.Dollar)
		sfl := psql.Select(`
			t1.table_name as mainName, t2.field_name,
			t2.alias, t3.type_name, t3.table_name,
			CASE WHEN t5.alias IS NULL THEN '' ELSE t5.alias END as alias_table`).
			From("metadata_table_vt AS t1").
			LeftJoin("field AS t2 ON t1.id = t2.table_id_vt").
			LeftJoin("field_type AS t3 ON t2.id = t3.field_id").
			LeftJoin("metadata_table_main AS t4 ON t3.table_name = t4.table_name").
			LeftJoin("metadata as t5 ON t4.metadata_id = t5.id").
			Where(sq.Eq{"t1.table_name": chilTable}).
			RunWith(pg)

		flrows, err := sfl.Query()
		if err != nil {
			return nil, err
		}
		defer flrows.Close()

		var tabName, fldName, alias, fldType, fldTable, fldTableAlias string
		flds := make(map[string]map[string]string, 0)
		for flrows.Next() {
			if err := flrows.Scan(&tabName, &fldName, &alias, &fldType, &fldTable, &fldTableAlias); err != nil {
				return nil, err
			}
			flds[fldName] = map[string]string{
				"alias":       alias,
				"type":        fldType,
				"table":       fldTable,
				"table_alias": fldTableAlias,
			}
		}

		rws := make([]map[string]string, 0, len(v))
		for i := range v {
			chRow := v[i]
			selector := make([]string, 0, len(flds))
			for k, i := range flds {
				typeName := i["type"]
				switch typeName {
				case "ref":
					selector = append(selector, fmt.Sprintf("CONVERT(VARCHAR(34), t1.%s, 1) AS %s", k, k))
				case "bool":
					selector = append(selector, fmt.Sprintf("CONVERT(VARCHAR(4),t1.%s,1) AS %s", k, k))
				case "string":
					selector = append(selector, fmt.Sprintf("t1.%s AS %s", k, k))
				case "int":
					selector = append(selector, fmt.Sprintf("t1.%s AS %s", k, k))
				case "date":
					selector = append(selector, fmt.Sprintf("FORMAT(t1.%s, 'dd.MM.yyyy HH:mm:ss', 'ru-RU') AS %s", k, k))
				default:
					return nil, fmt.Errorf("Unsupported type of field, %s %s", k, typeName)
				}
			}

			mainT := strings.Split(chilTable, "_")[1]
			sel := sq.Select(strings.Join(selector, ",")).
				From("dbo." + tabName + " AS t1").
				Where(sq.Eq{
					fmt.Sprintf("CONVERT(VARCHAR(34), t1._%s_IDRRef, 2)", mainT): ref,
					"CONVERT(VARCHAR(10), t1._KeyField, 2)": chRow["_keyField"],
				}).RunWith(ms)

			rows, err := sel.Query()

			if err != nil {
				return nil, err
			}
			res, err := dbms.ReadRow(rows)
			if err != nil {
				return nil, err
			}
			arJoin := make([]string, 0, len(res))
			selector = make([]string, 0, len(res))

			tableCounter := 2
			enums := make(map[string]string)
			for kf, vf := range res {
				sets := flds[kf]
				tabl := sets["table"]
				typeName := sets["type"]
				tablAlias := sets["table_alias"]

				switch typeName {
				case "ref":
					if vf == vlRefEmpty {
						selector = append(selector, fmt.Sprintf("'' AS %s", kf))
						continue
					}

					if is, flnum := isCompositeField(kf, res); is {
						fldT := fmt.Sprintf("_Fld%s_TYPE", flnum)
						fldRT := fmt.Sprintf("_Fld%s_RTRef", flnum)
						fldRef := fmt.Sprintf("_Fld%s_RRRef", flnum)
						refV := res[fldRef]

						if refV == vlRefEmpty {
							selector = append(selector, fmt.Sprintf("'' AS %s", kf))
							continue
						}

						typeField := res[fldT]
						switch typeField {
						case fldTypeBool:
							fld := fmt.Sprintf("_Fld%s_L", flnum)
							selector = append(selector, fmt.Sprintf("CASE WHEN t1.%s = 0x00 THEN 0 ELSE 1 END AS %s", fld, kf))
						case fldTypeInt:
							fld := fmt.Sprintf("_Fld%s_N", flnum)
							selector = append(selector, fmt.Sprintf("t1.%s AS %s", fld, kf))
						case fldTypeDate:
							fld := fmt.Sprintf("_Fld%s_", flnum)
							selector = append(selector, fmt.Sprintf("FORMAT(t1.%s, 'dd.MM.yyyy HH:mm:ss', 'ru-RU') AS %s", fld, fldRef))
						case fldTypeString:
							fld := fmt.Sprintf("_Fld%s_S", flnum)
							selector = append(selector, fmt.Sprintf("t1.%s AS %s", fld, fldRef))
						case fldTypeRef:
							valRT := res[fldRT]
							numTab, err := strconv.ParseInt(valRT[2:], 16, 64)
							if err != nil {
								return nil, err
							}
							psql := sq.StatementBuilder.PlaceholderFormat(sq.Dollar)
							sl := psql.Select("table_name").From("metadata_table_main").Where(sq.Eq{"table_number": numTab}).RunWith(pg)
							var tableName string
							if err := sl.QueryRow().Scan(&tableName); err != nil {
								return nil, err
							}
							arJoin = append(arJoin,
								fmt.Sprintf("dbo.%s AS t%d ON t1.%s = t%d._IDRRef", tableName, tableCounter, fldRef, tableCounter))

							if strings.Contains(tableName, "Document") {
								selector = append(selector, fmt.Sprintf(
									"CONCAT('%s № ', t%d._Number, ' от ',   FORMAT(t%d._Date_Time, 'dd.MM.yyyy HH:mm:ss', 'ru-RU')) AS %s",
									tablAlias, tableCounter, tableCounter, kf))
							} else if strings.Contains(tableName, "Reference") {
								selector = append(selector, fmt.Sprintf("t%d._Description AS %s", tableCounter, fldRef))
							} else if strings.Contains(tableName, "Enum") {
								selector = append(selector, fmt.Sprintf("t%d._EnumOrder AS %s", tableCounter, fldRef))
								enums[fldRef] = tableName
							}

							tableCounter++
						}
						delete(res, fldT)
						delete(res, fldRef)
						delete(res, fldRT)
					} else {
						arJoin = append(arJoin,
							fmt.Sprintf("dbo.%s AS t%d ON t1.%s = t%d._IDRRef", tabl, tableCounter, kf, tableCounter))

						if strings.Contains(tabl, "Document") {
							selector = append(selector, fmt.Sprintf(
								"CONCAT('%s № ', t%d._Number, ' от ',   FORMAT(t%d._Date_Time, 'dd.MM.yyyy HH:mm:ss', 'ru-RU')) AS %s",
								tablAlias, tableCounter, tableCounter, kf))
						} else if strings.Contains(tabl, "Reference") {
							selector = append(selector, fmt.Sprintf("t%d._Description AS %s", tableCounter, kf))
						} else if strings.Contains(tabl, "Enum") {
							selector = append(selector, fmt.Sprintf("t%d._EnumOrder AS %s", tableCounter, kf))
							enums[kf] = tabl
						}
						tableCounter++
					}
				case "bool":
					selector = append(selector, fmt.Sprintf("CASE WHEN t1.%s = 0x00 THEN 0 ELSE 1 END AS %s", kf, kf))
				case "string":
					selector = append(selector, fmt.Sprintf("t1.%s AS %s", kf, kf))
				case "int":
					selector = append(selector, fmt.Sprintf("t1.%s AS %s", kf, kf))
				case "date":
					selector = append(selector, fmt.Sprintf("FORMAT(t1.%s, 'dd.MM.yyyy HH:mm:ss', 'ru-RU') AS %s", kf, kf))
				default:
					return nil, fmt.Errorf("Unsupported type of field, %s %s", kf, typeName)
				}
			}
			selector = append(selector, "CONVERT(VARCHAR(10), t1._KeyField, 2) AS keyField")
			base := sq.Select(strings.Join(selector, ",")).From("dbo." + tabName + " AS t1")
			for _, tj := range arJoin {
				base = base.LeftJoin(tj)
			}
			base = base.Where(sq.Eq{
				fmt.Sprintf("CONVERT(VARCHAR(34), t1._%s_IDRRef, 2)", mainT) : ref,
				"CONVERT(VARCHAR(10), t1._KeyField, 2)": chRow["_keyField"],
				}).RunWith(ms)
			
			row, err := base.Query()
			if err != nil {
				return nil, err
			}
			record, err := dbms.ReadRow(row)
			if err != nil {
				return nil, err
			}
			
			if len(enums) > 0 {
				for k, v := range enums {
					slen := psql.Select("t2.alias").
						From("enums AS t1").
						LeftJoin("enums_value AS t2 ON t1.id = t2.enum_id").
						Where(sq.Eq{"t1.enum_name": v, "t2.order_enum": record[k]}).
						RunWith(pg)

					var enumAlias string
					if err := slen.QueryRow().Scan(&enumAlias); err != nil {
						return nil, err
					}
					record[k] = enumAlias
				}
			}
			prettyRecord := make(map[string]string)
			for k, v := range flds {
				if val, ok := record[k]; ok {
					prettyRecord[v["alias"]] = val
				} else {
					if strings.Contains(k, "_TYPE") && strings.Contains(k, "_RTRef") {
						continue
					}
				}

			}
			prettyRecord[fldNumber] = record["keyField"]
			rws = append(rws, prettyRecord)
		}
		vts[chilTable] = rws

	}
	
	return vts, nil

}

func isCompositeField(fld string, res map[string]string) (is bool, fldName string) {
	flNum := regexp.MustCompile(`[\d]{1,7}`).FindString(fld)
	fldOfType := fmt.Sprintf("_Fld%s_TYPE", flNum)
	for k := range res {
		if k == fldOfType {
			return true, flNum
		}
	}
	return false, ""
}
