package cdc

import (
	"encoding/json"
	"fmt"
)

// EventType represents the type of the CDC event.
type EventType int

const (
	TypeDDLEvent EventType = iota
	TypeDMLEvent
)

// Event represents data changes within MariaDB(DDL or DML).
type Event interface {
	// Type returns the type of data change captured.
	Type() EventType
	// GTID returns the GTID associated to the event.
	GTID() string
}

// DDL event represents data definition events(CREATE TABLE, ALTER TABLE).
//
// See: https://github.com/mariadb-corporation/MaxScale/blob/6.4/Documentation/Routers/KafkaCDC.md
type DDLEvent struct {
	Namespace string `json:"namespace"`
	EventType string `json:"type"`
	Name      string `json:"name"`
	// Table is the name of the table associated to the event.
	Table string `json:"table"`
	// Database is the database the table is in.
	Database string `json:"database"`
	// Version is the schema version, incremented when the table format changes.
	Version int `json:"version"`
	// EventGTID is the GTID that created the current version of the table.
	EventGTID string          `json:"gtid"`
	Fields    []DDLEventField `json:"fields"`
}

func (*DDLEvent) Type() EventType { return TypeDDLEvent }

func (e *DDLEvent) GTID() string { return e.EventGTID }

type DDLEventField struct {
	// Name is the field name.
	Name string            `json:"name"`
	Type DDLEventFieldType `json:"type"`
	// RealType represents the field type.
	RealType string `json:"real_type,omitempty"`
	// Length is the field length, if found.
	Length int `json:"length,omitempty"`
	// Unsigned tells wether the field is unsigned or not.
	Unsigned bool `json:"unsigned,omitempty"`
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

type DDLEventFieldTypeType int

const (
	TypeDDLEventFieldTypeArray DDLEventFieldTypeType = iota
	TypeDDLEventFieldTypeEnum
	TypeDDLEventFieldTypeString
)

type DDLEventFieldType interface {
	Type() DDLEventFieldTypeType
}

type DDLEventFieldTypeTableData []string

func (DDLEventFieldTypeTableData) Type() DDLEventFieldTypeType { return TypeDDLEventFieldTypeArray }

type DDLEventFieldTypeEnum struct {
	FieldType string   `json:"type"`
	Name      string   `json:"name"`
	Symbols   []string `json:"symbols"`
}

func (DDLEventFieldTypeEnum) Type() DDLEventFieldTypeType { return TypeDDLEventFieldTypeEnum }

type DDLEventFieldTypeString string

func (DDLEventFieldTypeString) Type() DDLEventFieldTypeType { return TypeDDLEventFieldTypeString }

// DMLEventType represents the type of the DML event.
type DMLEventType string

const (
	// DMLEventTypeInsert is the type of the DML event which represents data that
	// was added to MariaDB.
	DMLEventTypeInsert DMLEventType = "insert"
	// DMLEventTypeDelete is the type of the DML event which represents data that
	// was removed from MariaDB.
	DMLEventTypeDelete DMLEventType = "delete"
	// DMLEventTypeUpdateBefore is the type of the DML event which contains
	// the data before an update statement modified it.
	DMLEventTypeUpdateBefore DMLEventType = "update_before"
	// DMLEventTypeUpdateAfter is the type of the DML event which contains
	// the data after an update statement modified it.
	DMLEventTypeUpdateAfter DMLEventType = "update_after"
)

// DMLEvent represents data manipulation events(INSERT, UPDATE, DELETE).
//
// See: https://github.com/mariadb-corporation/MaxScale/blob/maxscale-6.2.4/Documentation/Routers/KafkaCDC.md#overview
type DMLEvent struct {
	// Domain is the first part of the GTID.
	// See https://mariadb.com/kb/en/gtid/#the-domain-id
	Domain int `json:"domain"`
	// ServerID is the second part of the GTID.
	// It is a unique number for each MariaDB/MySQL server.
	ServerID int `json:"server_id"`
	// Sequence is the third part of the GTID.
	// It is a monotonically increasing integer for each event group.
	Sequence int `json:"sequence"`
	// EventNumber is the sequence number of events inside the transaction
	// starting from 1.
	EventNumber int `json:"event_number"`
	// Timestamp is the UNIX timestamp when the event occurred.
	Timestamp int64 `json:"timestamp"`
	// EventType is the type of the event.
	// It can either be `insert`, `delete`, `updated_before` or `update_after`.
	EventType DMLEventType `json:"event_type"`
	// TableName is the name of the table associated to the event.
	TableName string `json:"table_name"`
	// TableSchema is the name of the database the table is in.
	TableSchema string `json:"table_schema"`
	// Raw is a JSON representation of the event the way it was received.
	Raw []byte `json:"raw"`
}

func (*DMLEvent) Type() EventType { return TypeDMLEvent }

func (e *DMLEvent) GTID() string {
	return fmt.Sprintf("%d-%d-%d", e.Domain, e.ServerID, e.Sequence)
}

func (e *DMLEvent) TableData() (map[string]interface{}, error) {
	var tableData map[string]interface{}
	if err := json.Unmarshal(e.Raw, &tableData); err != nil {
		return nil, fmt.Errorf("could not unmarshal the raw DML event: %w", err)
	}

	// Table data are all the fields that are not part of those below.
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
