//go:build integration

package cdc_test

import (
	"encoding/json"
	"net"
	"os"
	"testing"

	cdc "github.com/connectfit-team/maxscale-cdc"
	"github.com/google/go-cmp/cmp"
)

type test struct {
	ID int `json:"id"`
}

func TestCDCClient_RequestData_FailsIfWrongAddress(t *testing.T) {
	client := cdc.NewClient("wrong address", "", "", "")

	_, err := client.RequestData("", "")
	if err == nil {
		t.Fatalf("Should return an error when given a wrong address: %v", err)
	}
}

func TestCDCClient_RequestData_FailsWithWrongCredentials(t *testing.T) {
	host, port := os.Getenv("MAXSCALE_HOST"), os.Getenv("MAXSCALE_PORT")
	addr := net.JoinHostPort(host, port)
	client := cdc.NewClient(addr, "wrong", "credentials", "")

	_, err := client.RequestData("", "")
	if err == nil {
		t.Fatalf("Should return an error when given wrong credentials: %v", err)
	}
}

func TestCDCClient_RequestData_FailsIfEmptyUUID(t *testing.T) {
	host, port := os.Getenv("MAXSCALE_HOST"), os.Getenv("MAXSCALE_PORT")
	addr := net.JoinHostPort(host, port)
	user, password := os.Getenv("MAXSCALE_USER"), os.Getenv("MAXSCALE_PASSWORD")
	client := cdc.NewClient(addr, user, password, "")

	_, err := client.RequestData("", "")
	if err == nil {
		t.Fatalf("Should return an error when given empty UUID: %v", err)
	}
}

func TestCDCClient_RequestData_DoesNotFailEvenIfTableDoesNotExist(t *testing.T) {
	host, port := os.Getenv("MAXSCALE_HOST"), os.Getenv("MAXSCALE_PORT")
	addr := net.JoinHostPort(host, port)
	user, password := os.Getenv("MAXSCALE_USER"), os.Getenv("MAXSCALE_PASSWORD")
	client := cdc.NewClient(addr, user, password, "test-uuid")

	_, err := client.RequestData("test", "bar")
	if err != nil {
		t.Fatalf("Should not return an error when given wrong database and table: %v", err)
	}
	defer client.Stop()
}

func TestCDCClient_RequestData(t *testing.T) {
	host, port := os.Getenv("MAXSCALE_HOST"), os.Getenv("MAXSCALE_PORT")
	addr := net.JoinHostPort(host, port)
	user, password := os.Getenv("MAXSCALE_USER"), os.Getenv("MAXSCALE_PASSWORD")
	client := cdc.NewClient(addr, user, password, "test-uuid")

	database, table := os.Getenv("MAXSCALE_DATABASE"), os.Getenv("MAXSCALE_TABLE")
	data, err := client.RequestData(database, table)
	if err != nil {
		t.Fatalf("Failed to request data: %v", err)
	}
	defer client.Stop()

	event := <-data
	expectedDDLEvent := &cdc.DDLEvent{
		Namespace: "MaxScaleChangeDataSchema.avro",
		EventType: "record",
		Name:      "ChangeRecord",
		Table:     "tests",
		Database:  "test",
		Version:   1,
		EventGTID: "0-3000-6",
		Fields: []cdc.DDLEventField{
			{
				Name: "domain",
				Type: cdc.DDLEventFieldTypeString("int"),
			},
			{
				Name: "server_id",
				Type: cdc.DDLEventFieldTypeString("int"),
			},
			{
				Name: "sequence",
				Type: cdc.DDLEventFieldTypeString("int"),
			},
			{
				Name: "event_number",
				Type: cdc.DDLEventFieldTypeString("int"),
			},
			{
				Name: "timestamp",
				Type: cdc.DDLEventFieldTypeString("int"),
			},
			{
				Name: "event_type",
				Type: cdc.DDLEventFieldTypeEnum{
					FieldType: "enum",
					Name:      "EVENT_TYPES",
					Symbols: []string{
						"insert",
						"update_before",
						"update_after",
						"delete",
					},
				},
			},
			{
				Name: "id",
				Type: cdc.DDLEventFieldTypeTableData{
					"null",
					"int",
				},
				RealType: "int",
				Length:   -1,
			},
		},
	}
	ddlEvent := event.(*cdc.DDLEvent)
	if !cmp.Equal(expectedDDLEvent, ddlEvent) {
		t.Fatalf("Captured DDL event differs from the expected one: %s", cmp.Diff(expectedDDLEvent, ddlEvent))
	}

	event = <-data
	dmlEvent := event.(*cdc.DMLEvent)
	var insertedRow test
	if err = json.Unmarshal(dmlEvent.Raw, &insertedRow); err != nil {
		t.Fatalf("Should be able to unmarshal the raw data from the captured event to JSON: %v", err)
	}

	// Cannot compare these fields since we cannot predict the exact timestamp
	dmlEvent.Timestamp = 0
	dmlEvent.Raw = nil

	expectedDMLEvent := &cdc.DMLEvent{
		Domain:      0,
		ServerID:    3000,
		Sequence:    7,
		EventNumber: 1,
		EventType:   "insert",
	}
	if !cmp.Equal(expectedDMLEvent, dmlEvent) {
		t.Fatalf("Captured DML event differs from the expected one:\n%s", cmp.Diff(expectedDMLEvent, dmlEvent))
	}

	expectedID := 1
	if insertedRow.ID != expectedID {
		t.Fatalf("The inserted row's id column should be equal to %d", expectedID)
	}

}

func TestCDCClient_RequestData_WithGTID(t *testing.T) {
	host, port := os.Getenv("MAXSCALE_HOST"), os.Getenv("MAXSCALE_PORT")
	addr := net.JoinHostPort(host, port)
	user, password := os.Getenv("MAXSCALE_USER"), os.Getenv("MAXSCALE_PASSWORD")
	client := cdc.NewClient(addr, user, password, "test-uuid")

	database, table := os.Getenv("MAXSCALE_DATABASE"), os.Getenv("MAXSCALE_TABLE")
	data, err := client.RequestData(database, table, cdc.WithGTID("0-3000-8"))
	if err != nil {
		t.Fatalf("Failed to request data: %v", err)
	}
	defer client.Stop()

	event := <-data
	expectedDDLEvent := &cdc.DDLEvent{
		Namespace: "MaxScaleChangeDataSchema.avro",
		EventType: "record",
		Name:      "ChangeRecord",
		Table:     "tests",
		Database:  "test",
		Version:   1,
		EventGTID: "0-3000-6",
		Fields: []cdc.DDLEventField{
			{
				Name: "domain",
				Type: cdc.DDLEventFieldTypeString("int"),
			},
			{
				Name: "server_id",
				Type: cdc.DDLEventFieldTypeString("int"),
			},
			{
				Name: "sequence",
				Type: cdc.DDLEventFieldTypeString("int"),
			},
			{
				Name: "event_number",
				Type: cdc.DDLEventFieldTypeString("int"),
			},
			{
				Name: "timestamp",
				Type: cdc.DDLEventFieldTypeString("int"),
			},
			{
				Name: "event_type",
				Type: cdc.DDLEventFieldTypeEnum{
					FieldType: "enum",
					Name:      "EVENT_TYPES",
					Symbols: []string{
						"insert",
						"update_before",
						"update_after",
						"delete",
					},
				},
			},
			{
				Name: "id",
				Type: cdc.DDLEventFieldTypeTableData{
					"null",
					"int",
				},
				RealType: "int",
				Length:   -1,
			},
		},
	}
	ddlEvent := event.(*cdc.DDLEvent)
	if !cmp.Equal(expectedDDLEvent, ddlEvent) {
		t.Fatalf("Captured DDL event differs from the expected one: %s", cmp.Diff(expectedDDLEvent, ddlEvent))
	}

	event = <-data
	dmlEvent := event.(*cdc.DMLEvent)
	var insertedRow test
	if err = json.Unmarshal(dmlEvent.Raw, &insertedRow); err != nil {
		t.Fatalf("Should be able to unmarshal the raw data from the captured event to JSON: %v", err)
	}

	// Cannot compare these fields since we cannot predict the exact timestamp
	dmlEvent.Timestamp = 0
	dmlEvent.Raw = nil

	expectedDMLEvent := &cdc.DMLEvent{
		Domain:      0,
		ServerID:    3000,
		Sequence:    8,
		EventNumber: 1,
		EventType:   "insert",
	}
	if !cmp.Equal(expectedDMLEvent, dmlEvent) {
		t.Fatalf("Captured DML event differs from the expected one:\n%s", cmp.Diff(expectedDMLEvent, dmlEvent))
	}

	expectedID := 2
	if insertedRow.ID != expectedID {
		t.Fatalf("The inserted row's id column should be equal to %d", expectedID)
	}
}
