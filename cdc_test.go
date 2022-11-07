//go:build integration

package maxscale_test

import (
	"context"
	"net"
	"os"
	"testing"

	"github.com/connectfit-team/maxscale-cdc-client"
	"github.com/google/go-cmp/cmp"
	"github.com/google/uuid"
)

func TestCDCClient_RequestData_FailsIfWrongAddress(t *testing.T) {
	ctx := context.Background()

	client := maxscale.NewCDCClient("wrong address", "", "", "")

	_, err := client.RequestData(ctx, "", "")
	if err == nil {
		t.Fatalf("Should return an error when given a wrong address: %v", err)
	}
}

func TestCDCClient_RequestData_FailsWithWrongCredentials(t *testing.T) {
	ctx := context.Background()

	host, port := os.Getenv("MAXSCALE_HOST"), os.Getenv("MAXSCALE_PORT")
	addr := net.JoinHostPort(host, port)
	client := maxscale.NewCDCClient(addr, "wrong", "credentials", "")

	_, err := client.RequestData(ctx, "", "")
	if err == nil {
		t.Fatalf("Should return an error when given wrong credentials: %v", err)
	}
}

func TestCDCClient_RequestData_FailsIfEmptyUUID(t *testing.T) {
	ctx := context.Background()

	host, port := os.Getenv("MAXSCALE_HOST"), os.Getenv("MAXSCALE_PORT")
	addr := net.JoinHostPort(host, port)
	user, password := os.Getenv("MAXSCALE_USER"), os.Getenv("MAXSCALE_PASSWORD")
	client := maxscale.NewCDCClient(addr, user, password, "")

	_, err := client.RequestData(ctx, "", "")
	if err == nil {
		t.Fatalf("Should return an error when given empty UUID: %v", err)
	}
}

func TestCDCClient_RequestData_DoesNotFailEvenIfTableDoesNotExist(t *testing.T) {
	host, port := os.Getenv("MAXSCALE_HOST"), os.Getenv("MAXSCALE_PORT")
	addr := net.JoinHostPort(host, port)
	user, password := os.Getenv("MAXSCALE_USER"), os.Getenv("MAXSCALE_PASSWORD")
	client := maxscale.NewCDCClient(addr, user, password, uuid.NewString())

	_, err := client.RequestData(context.Background(), "test", "bar")
	if err != nil {
		t.Fatalf("Should not return an error when given wrong database and table: %v", err)
	}
	defer client.Close()
}

func TestCDCClient_RequestData(t *testing.T) {
	host, port := os.Getenv("MAXSCALE_HOST"), os.Getenv("MAXSCALE_PORT")
	addr := net.JoinHostPort(host, port)
	user, password := os.Getenv("MAXSCALE_USER"), os.Getenv("MAXSCALE_PASSWORD")
	client := maxscale.NewCDCClient(addr, user, password, uuid.NewString())

	database, table := os.Getenv("MAXSCALE_DATABASE"), os.Getenv("MAXSCALE_TABLE")
	data, err := client.RequestData(context.Background(), database, table)
	if err != nil {
		t.Fatalf("Failed to request data: %v", err)
	}
	defer client.Close()

	event := <-data
	expectedDDLEvent := &maxscale.DDLEvent{
		Namespace: "MaxScaleChangeDataSchema.avro",
		EventType: "record",
		Name:      "ChangeRecord",
		Table:     "tests",
		Database:  "test",
		Version:   1,
		EventGTID: "0-3000-6",
		Fields: []maxscale.DDLEventField{
			{
				Name: "domain",
				Type: maxscale.DDLEventFieldTypeString("int"),
			},
			{
				Name: "server_id",
				Type: maxscale.DDLEventFieldTypeString("int"),
			},
			{
				Name: "sequence",
				Type: maxscale.DDLEventFieldTypeString("int"),
			},
			{
				Name: "event_number",
				Type: maxscale.DDLEventFieldTypeString("int"),
			},
			{
				Name: "timestamp",
				Type: maxscale.DDLEventFieldTypeString("int"),
			},
			{
				Name: "event_type",
				Type: maxscale.DDLEventFieldTypeEnum{
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
				Type: maxscale.DDLEventFieldTypeTableData{
					"null",
					"int",
				},
				RealType: "int",
				Length:   -1,
			},
		},
	}
	ddlEvent := event.(*maxscale.DDLEvent)
	if !cmp.Equal(expectedDDLEvent, ddlEvent) {
		t.Fatalf("Captured DDL event differs from the expected one: %s", cmp.Diff(expectedDDLEvent, ddlEvent))
	}

	event = <-data
	expectedDMLEvent := &maxscale.DMLEvent{
		Domain:      0,
		ServerID:    3000,
		Sequence:    7,
		EventNumber: 1,
		EventType:   "insert",
		TableData: map[string]interface{}{
			"id": float64(1),
		},
	}
	dmlEvent := event.(*maxscale.DMLEvent)
	// TODO: Find a way to compare timestamp as well
	dmlEvent.Timestamp = 0
	if !cmp.Equal(expectedDMLEvent, dmlEvent) {
		t.Fatalf("Captured DML event differs from the expected one:\n%s", cmp.Diff(expectedDMLEvent, dmlEvent))
	}
}

func TestCDCClient_RequestData_WithGTID(t *testing.T) {
	host, port := os.Getenv("MAXSCALE_HOST"), os.Getenv("MAXSCALE_PORT")
	addr := net.JoinHostPort(host, port)
	user, password := os.Getenv("MAXSCALE_USER"), os.Getenv("MAXSCALE_PASSWORD")
	client := maxscale.NewCDCClient(addr, user, password, uuid.NewString())

	database, table := os.Getenv("MAXSCALE_DATABASE"), os.Getenv("MAXSCALE_TABLE")
	data, err := client.RequestData(context.Background(), database, table, maxscale.WithGTID("0-3000-8"))
	if err != nil {
		t.Fatalf("Failed to request data: %v", err)
	}
	defer client.Close()

	event := <-data
	expectedDDLEvent := &maxscale.DDLEvent{
		Namespace: "MaxScaleChangeDataSchema.avro",
		EventType: "record",
		Name:      "ChangeRecord",
		Table:     "tests",
		Database:  "test",
		Version:   1,
		EventGTID: "0-3000-6",
		Fields: []maxscale.DDLEventField{
			{
				Name: "domain",
				Type: maxscale.DDLEventFieldTypeString("int"),
			},
			{
				Name: "server_id",
				Type: maxscale.DDLEventFieldTypeString("int"),
			},
			{
				Name: "sequence",
				Type: maxscale.DDLEventFieldTypeString("int"),
			},
			{
				Name: "event_number",
				Type: maxscale.DDLEventFieldTypeString("int"),
			},
			{
				Name: "timestamp",
				Type: maxscale.DDLEventFieldTypeString("int"),
			},
			{
				Name: "event_type",
				Type: maxscale.DDLEventFieldTypeEnum{
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
				Type: maxscale.DDLEventFieldTypeTableData{
					"null",
					"int",
				},
				RealType: "int",
				Length:   -1,
			},
		},
	}
	ddlEvent := event.(*maxscale.DDLEvent)
	if !cmp.Equal(expectedDDLEvent, ddlEvent) {
		t.Fatalf("Captured DDL event differs from the expected one: %s", cmp.Diff(expectedDDLEvent, ddlEvent))
	}

	event = <-data
	expectedDMLEvent := &maxscale.DMLEvent{
		Domain:      0,
		ServerID:    3000,
		Sequence:    8,
		EventNumber: 1,
		EventType:   "insert",
		TableData: map[string]interface{}{
			"id": float64(2),
		},
	}
	dmlEvent := event.(*maxscale.DMLEvent)
	// TODO: Find a way to compare timestamp as well
	dmlEvent.Timestamp = 0
	if !cmp.Equal(expectedDMLEvent, dmlEvent) {
		t.Fatalf("Captured DML event differs from the expected one:\n%s", cmp.Diff(expectedDMLEvent, dmlEvent))
	}
}
