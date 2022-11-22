package cdc

import (
	"encoding/json"
	"fmt"
)

const (
	TypeDDLEvent = iota
	TypeDMLEvent
)

// Event represents data changes within MariaDB(DDL or DML).
type Event interface {
	// Type returns the type of data change captured.
	Type() int
	// GTID returns the GTID associated to the event.
	GTID() string
}

// DDL event represents data definition events(CREATE TABLE, ALTER TABLE).
//
// See: https://github.com/mariadb-corporation/MaxScale/blob/6.4/Documentation/Routers/KafkaCDC.md
type DDLEvent struct {
	Namespace string          `json:"namespace"`
	EventType string          `json:"type"`
	Name      string          `json:"name"`
	Table     string          `json:"table"`
	Database  string          `json:"database"`
	Version   int             `json:"version"`
	EventGTID string          `json:"gtid"`
	Fields    []DDLEventField `json:"fields"`
}

func (*DDLEvent) Type() int { return TypeDDLEvent }

func (e *DDLEvent) GTID() string { return e.EventGTID }

type DDLEventField struct {
	Name     string            `json:"name"`
	Type     DDLEventFieldType `json:"type"`
	RealType string            `json:"real_type,omitempty"`
	Length   int               `json:"length,omitempty"`
	Unsigned bool              `json:"unsigned,omitempty"`
}

func (f *DDLEventField) UnmarshalJSON(data []byte) error {
	// Data in the `type` field differ from one to the other so we need to
	// marshal them in an `interface{}` first and then handle them case by case.
	//
	// e.g.
	// {
	// 	"name": "domain",
	// 	"type": "int"
	// },
	//
	// {
	// 	"name": "id",
	// 	"type": [
	// 		"null",
	// 		"long"
	// 	],
	// 	"real_type": "int",
	// 	"length": -1,
	// 	"unsigned": false
	// }
	//
	// {
	// 	"name": "event_type",
	// 	"type": {
	// 		"type": "enum",
	// 		"name": "EVENT_TYPES",
	// 		"symbols": [
	// 			"insert",
	// 			"update_before",
	// 			"update_after",
	// 			"delete"
	// 		]
	// 	}
	var field struct {
		Name     string      `json:"name"`
		Type     interface{} `json:"type"`
		RealType string      `json:"real_type,omitempty"`
		Length   int         `json:"length,omitempty"`
		Unsigned bool        `json:"unsigned,omitempty"`
	}
	if err := json.Unmarshal(data, &field); err != nil {
		return err
	}
	f.Name = field.Name
	f.RealType = field.RealType
	f.Length = field.Length
	f.Unsigned = field.Unsigned

	switch value := field.Type.(type) {
	case string:
		f.Type = DDLEventFieldTypeString(value)

	case []interface{}:
		var tableDataType DDLEventFieldTypeTableData
		for _, elem := range value {
			s, ok := elem.(string)
			if !ok {
				return fmt.Errorf("invalid type %T in table data's field type", elem)
			}
			tableDataType = append(tableDataType, s)
		}
		f.Type = tableDataType

	case map[string]interface{}:
		var enumType DDLEventFieldTypeEnum
		b, err := json.Marshal(value)
		if err != nil {
			return err
		}
		if err := json.Unmarshal(b, &enumType); err != nil {
			return err
		}
		f.Type = enumType

	default:
		return fmt.Errorf("unknown type %T in event field type definition", value)
	}

	return nil
}

const (
	TypeDDLEventFieldTypeTableData = iota
	TypeDDLEventFieldTypeEvent
	TypeDDLEventFieldFieldString
)

type DDLEventFieldType interface {
	Type() int
}

type DDLEventFieldTypeTableData []string

func (DDLEventFieldTypeTableData) Type() int { return TypeDDLEventFieldTypeTableData }

type DDLEventFieldTypeEnum struct {
	FieldType string   `json:"type"`
	Name      string   `json:"name"`
	Symbols   []string `json:"symbols"`
}

func (DDLEventFieldTypeEnum) Type() int { return TypeDDLEventFieldTypeEvent }

type DDLEventFieldTypeString string

func (DDLEventFieldTypeString) Type() int { return TypeDDLEventFieldFieldString }

// DMLEvent represents data manipulation events(INSERT, UPDATE, DELETE).
//
// See: https://github.com/mariadb-corporation/MaxScale/blob/maxscale-6.2.4/Documentation/Routers/KafkaCDC.md#overview
type DMLEvent struct {
	Domain      int    `json:"domain"`
	ServerID    int    `json:"server_id"`
	Sequence    int    `json:"sequence"`
	EventNumber int    `json:"event_number"`
	Timestamp   int64  `json:"timestamp"`
	EventType   string `json:"event_type"`
	TableName   string `json:"table_name"`
	TableSchema string `json:"table_schema"`
	Raw         []byte `json:"raw"`
}

func (*DMLEvent) Type() int { return TypeDMLEvent }

func (e *DMLEvent) GTID() string {
	return fmt.Sprintf("%d-%d-%d", e.Domain, e.ServerID, e.Sequence)
}

func (e *DMLEvent) TableData() (map[string]interface{}, error) {
	var tableData map[string]interface{}
	if err := json.Unmarshal(e.Raw, &tableData); err != nil {
		return nil, fmt.Errorf("could not unmarshal the raw DML event: %w", err)
	}
	delete(tableData, "domain")
	delete(tableData, "server_id")
	delete(tableData, "sequence")
	delete(tableData, "event_number")
	delete(tableData, "timestamp")
	delete(tableData, "event_type")
	delete(tableData, "table_name")
	delete(tableData, "table_schema")
	return tableData, nil
}
