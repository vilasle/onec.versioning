package mssql

import "fmt"

type SQLExec string

var (
	fref  string = "ref"
	fdate string = "date"
	fbool string = "bool"
	fstr  string = "string"
	fint  string = "int"
)

func (s SQLExec) Get(kind string, tableAlias string) string {
	switch kind{
	case fref:
		return s.forRef(tableAlias) 
	case fbool:
		return s.forBool(tableAlias)
	case fdate:
		return s.forDate(tableAlias)
	case fint:
		return s.forInt(tableAlias)
	case fstr:
		return s.forString(tableAlias)
	default:
		return string(s)
	}
}  

func (s SQLExec) forRef(tbAlias string) string {
	return fmt.Sprintf("CONVERT(VARCHAR(34), %s.%s, 2) AS %s", tbAlias, s, s)
}

func (s SQLExec) forBool(tbAlias string) string {
	return fmt.Sprintf("CASE WHEN %s.%s = 0x01 THEN 'true' ELSE 'false' END AS %s", tbAlias, s, s)
}

func (s SQLExec) forDate(tbAlias string) string {
	return fmt.Sprintf("FORMAT(%s.%s, 'dd.MM.yyyy hh:mm:ss', 'ru-RU') as %s", tbAlias, s, s)
}

func (s SQLExec) forString(tbAlias string) string {
	return fmt.Sprintf("%s.%s AS %s", tbAlias, s, s)
}

func (s SQLExec) forInt(tbAlias string) string {
	return fmt.Sprintf("%s.%s AS %s", tbAlias, s, s)
}
