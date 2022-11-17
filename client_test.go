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
	address := "wrong address"
	client := cdc.NewClient(address, "", "", "")

	_, err := client.RequestData("", "")
	if err == nil { // if NO error
		t.Errorf("No error was returned after trying to connect at \"%s\": %v", address, err)
	}
}

func TestCDCClient_RequestData_FailsWithWrongCredentials(t *testing.T) {
	host, port := os.Getenv("MAXSCALE_HOST"), os.Getenv("MAXSCALE_PORT")
	addr := net.JoinHostPort(host, port)
	user, password := "wrong", "credentials"
	client := cdc.NewClient(addr, user, password, "")

	_, err := client.RequestData("", "")
	if err == nil { // if NO error
		t.Errorf("No error was returned after trying to authenticate with user \"%s\" and password \"%s\": %v", user, password, err)
	}
}

func TestCDCClient_RequestData_FailsIfEmptyUUID(t *testing.T) {
	host, port := os.Getenv("MAXSCALE_HOST"), os.Getenv("MAXSCALE_PORT")
	addr := net.JoinHostPort(host, port)
	user, password := os.Getenv("MAXSCALE_USER"), os.Getenv("MAXSCALE_PASSWORD")
	client := cdc.NewClient(addr, user, password, "")

	_, err := client.RequestData("", "")
	if err == nil { // if NO error
		t.Errorf("No error was returned after trying to register without any UUID: %v", err)
	}
}

func TestCDCClient_RequestData_DoesNotFailEvenIfTableDoesNotExist(t *testing.T) {
	host, port := os.Getenv("MAXSCALE_HOST"), os.Getenv("MAXSCALE_PORT")
	addr := net.JoinHostPort(host, port)
	user, password := os.Getenv("MAXSCALE_USER"), os.Getenv("MAXSCALE_PASSWORD")
	uuid := "test-uuid"
	client := cdc.NewClient(addr, user, password, uuid)

	database, table := "test", "bar"
	_, err := client.RequestData(database, table)
	if err != nil {
		t.Errorf("An error was returned when requested data from table %s.%s but it should continue listening: %v", database, table, err)
	}
	defer client.Stop()
}

func TestCDCClient_RequestData(t *testing.T) {
	host, port := os.Getenv("MAXSCALE_HOST"), os.Getenv("MAXSCALE_PORT")
	addr := net.JoinHostPort(host, port)
	user, password := os.Getenv("MAXSCALE_USER"), os.Getenv("MAXSCALE_PASSWORD")
	uuid := "test-uuid"
	client := cdc.NewClient(addr, user, password, uuid)

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
	if diff := cmp.Diff(ddlEvent, expectedDDLEvent); diff != "" {
		t.Errorf("Captured DDL event differs from the expected one: %s", cmp.Diff(expectedDDLEvent, ddlEvent))
	}

	event = <-data
	dmlEvent := event.(*cdc.DMLEvent)
	var insertedRow test
	if err = json.Unmarshal(dmlEvent.Raw, &insertedRow); err != nil {
		t.Errorf("Should be able to unmarshal the raw data from the captured event to JSON: %v", err)
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
	if diff := cmp.Diff(dmlEvent, expectedDMLEvent); diff != "" {
		t.Errorf("Captured DML event differs from the expected one:\n%s", diff)
	}

	expectedID := 1
	if insertedRow.ID != expectedID {
		t.Errorf("The inserted row's id column should be equal to %d", expectedID)
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
	if diff := cmp.Diff(ddlEvent, expectedDDLEvent); diff != "" {
		t.Errorf("Captured DDL event differs from the expected one: %s", diff)
	}

	event = <-data
	dmlEvent := event.(*cdc.DMLEvent)
	var insertedRow test
	if err = json.Unmarshal(dmlEvent.Raw, &insertedRow); err != nil {
		t.Errorf("Should be able to unmarshal the raw data from the captured event to JSON: %v", err)
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
	if diff := cmp.Diff(dmlEvent, expectedDMLEvent); diff != "" {
		t.Errorf("Captured DML event differs from the expected one:\n%s", diff)
	}

	expectedID := 2
	if insertedRow.ID != expectedID {
		t.Errorf("The inserted row's id column should be equal to %d", expectedID)
	}
}
