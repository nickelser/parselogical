package parselogical

import (
	"fmt"
	"strings"
)

import (
	"github.com/pkg/errors"
)

const (
	// prelude section
	parseSchema uint32 = iota
	parseTable
	parseOp

	// field data section
	parseColumnName
	parseColumnType
	parseColumnValue
)

// ColumnValue is an annotated String type, containing the Postgres type of the String,
// and whether it was quoted within the original parsing.
// Quoting is useful for handling values like val[text]:null or val[text]:unchanged-toast-datum,
// which are special signifiers (denoting null and unchanged data, respectively).
type ColumnValue struct {
	String string
	Type   string
	Quoted bool
}

// ParsedTestDecoding is the result of parsing the string
type ParsedTestDecoding struct {
	unparsed *string // for internal use

	Transaction string // filled if this is a transaction statement (BEGIN/COMMIT)
	Schema      string // filled if this is a DML statement with the name of the schema
	Table       string // filled if this is a DML statement with the name of the table
	SchemaTable string // filled if this is a DML statement with the joined name (<schema>.<table>) for convenience
	Operation   string // filled if this is a DML statement with the name of the operation (INSERT/UPDATE/DELETE)

	Fields    map[string]ColumnValue // if a DML statement, the fields affected by the operation with their values and types
	OldFields map[string]ColumnValue // if an UPDATE and REPLICA IDENTITY setting (FULL or USING INDEX both will add values here)
}

// NewParsedTestDecoding creates the structure that is filled by the Parse operation
func NewParsedTestDecoding(msg string) *ParsedTestDecoding {
	ptd := new(ParsedTestDecoding)
	ptd.unparsed = &msg
	ptd.Fields = make(map[string]ColumnValue)
	ptd.OldFields = make(map[string]ColumnValue)
	return ptd
}

// Parse parses a string produced from the test_decoding logical replication plugin into the PTD structure, above
func (parsed *ParsedTestDecoding) Parse() error {
	err := parsed.ParsePrelude()
	if err != nil {
		return err
	}
	return parsed.ParseColumns()
}

// ParsePrelude allows more control over the parsing process
// if you want to avoid parsing the string for tables/schemas (via a filtering mechansim)
// you can run ParsePrelude to just fill the operation type, the schema and the table
func (parsed *ParsedTestDecoding) ParsePrelude() error {
	if len(*parsed.unparsed) < 5 {
		return errors.Errorf("message too short: '%s'", string(*parsed.unparsed))
	}

	switch (*parsed.unparsed)[0:5] {
	case "BEGIN":
		fallthrough
	case "COMMI":
		parsed.Transaction = string(*parsed.unparsed)
		return nil
	case "table":
		// hooray!
	default:
		return errors.Errorf("unknown logical message received: '%s'", string(*parsed.unparsed))
	}

	state := parseSchema
	s := (*parsed.unparsed)[6:]
	done := false

	// parses the following:
	// <schemaname>.<tablename>: <op>:
	for !done {
		for i := 0; i < len(s); i++ {
			oldState := state

			switch state {
			// <schemaname>.<tablename>: <op>:
			//             ^ parses up to here
			case parseSchema:
				if s[i] == '.' {
					parsed.Schema = string(s[0:i])
					s = s[i+1:] // skip the '.'
					state = parseTable
				}
				// <schemaname>.<tablename>: <op>:
				//                         ^ parses up to here
			case parseTable:
				if s[i] == ':' {
					parsed.Table = string(s[0:i])
					parsed.SchemaTable = fmt.Sprintf("%s.%s", parsed.Schema, parsed.Table)
					s = s[i+2:] // skip the space, too
					state = parseOp
				}
				// <schemaname>.<tablename>: <op>:
				//                               ^ parses up to here
			case parseOp:
				if s[i] == ':' {
					parsed.Operation = string(s[0:i])
					s = s[i+2:] // skip the space, too
					state = parseColumnName
					done = true
				}
			}

			if i >= (len(s) - 1) {
				done = true
				break
			}

			if oldState != state {
				break
			}
		}
	}

	if state != parseColumnName {
		panic("ended in an invalid state")
	}

	parsed.unparsed = &s

	return nil
}

// ParseColumns also does some cool shit
func (parsed *ParsedTestDecoding) ParseColumns() error {
	// BEGIN 30404
	// table public.users: INSERT: id[integer]:568543 active[boolean]:false created_at[timestamp without time zone]:'2017-05-08 16:35:17.076434'
	// COMMIT 30404

	if parsed.Transaction != "" {
		// no need to continue parsing if it's a transaction type
		return nil
	}

	// now we handle the new and old tuple cases
	// table public.users: UPDATE: old-key: id[integer]:568544 active[boolean]:true created_at[timestamp without time zone]:'2017-05-08 16:45:18.120996' new-tuple: id[integer]:568544 active[boolean]:true created_at[timestamp without time zone]:'2017-05-08 16:45:18.120996'
	if parsed.Operation == "UPDATE" {
		oldTupleIdx := strings.Index(*parsed.unparsed, "old-key: ")
		newTupleIdx := strings.Index(*parsed.unparsed, "new-tuple: ")

		if oldTupleIdx > -1 && newTupleIdx > -1 {
			err := parseTupleColumns((*parsed.unparsed)[oldTupleIdx+9:newTupleIdx-1], &parsed.OldFields)
			if err != nil {
				return err
			}
			return parseTupleColumns((*parsed.unparsed)[newTupleIdx+11:], &parsed.Fields)
		} else if oldTupleIdx > -1 {
			return parseTupleColumns((*parsed.unparsed)[oldTupleIdx+9:], &parsed.OldFields)
		} else if newTupleIdx > -1 {
			return parseTupleColumns((*parsed.unparsed)[newTupleIdx+11:], &parsed.Fields)
		}
	}

	return parseTupleColumns(*parsed.unparsed, &parsed.Fields)
}

func parseTupleColumns(tuple string, fields *map[string]ColumnValue) error {
	var columnName string
	var columnType string
	var inQuote bool
	var inDoubleQuote bool

	state := parseColumnName
	s := tuple
	done := false

	for !done {
		for i := 0; i < len(s); i++ {
			oldState := state
			lastChar := i == (len(s) - 1)

			switch state {
			case parseColumnName:
				if s[i] == '[' {
					columnName = string(s[0:i])
					s = s[i+1:]
					state = parseColumnType
				} else if s[i] == '(' && s[i:len(s)] == "(no-tuple-data)" {
					return nil
				}
			case parseColumnType:
				if s[i] == ']' {
					columnType = string(s[0:i])
					s = s[i+2:] // skip ':' too
					state = parseColumnValue
					inQuote = false
					inDoubleQuote = false
				}
			case parseColumnValue:
				if s[i] == '\'' {
					if i == 0 {
						inQuote = true
						s = s[i+1:]
						break
					} else if !inDoubleQuote && !lastChar && s[i+1] == '\'' {
						inDoubleQuote = true
					} else if inDoubleQuote {
						inDoubleQuote = false
					} else if inQuote && !inDoubleQuote {
						(*fields)[columnName] = ColumnValue{String: string(s[0:i]), Quoted: inQuote, Type: columnType}
						state = parseColumnName

						if !lastChar {
							s = s[i+2:]
						}
					}
				} else if (s[i] == ' ' || lastChar) && !inQuote {
					includeLastChar := 0
					if lastChar {
						includeLastChar = 1
					}
					(*fields)[columnName] = ColumnValue{String: string(s[0 : i+includeLastChar]), Quoted: inQuote, Type: columnType}
					state = parseColumnName

					if !lastChar {
						s = s[i+1:]
					}
				}
			}

			if lastChar {
				done = true
				break
			}

			if oldState != state {
				break
			}
		}
	}

	return nil
}
