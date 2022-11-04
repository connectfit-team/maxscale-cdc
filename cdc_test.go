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

func TestCDCClient_Connect(t *testing.T) {
	ctx := context.Background()

	host, port := os.Getenv("MAXSCALE_HOST"), os.Getenv("MAXSCALE_PORT")
	addr := net.JoinHostPort(host, port)
	conn, err := maxscale.Connect(ctx, addr)
	if err != nil {
		t.Fatalf("Failed to connect to the MaxScale CDC listener at %s: %v", addr, err)
	}
	defer conn.Close()
}

func TestCDCClient_Connect_FailsIfWrongAddress(t *testing.T) {
	ctx := context.Background()

	_, err := maxscale.Connect(ctx, "wrong address (:")
	if err == nil {
		t.Fatalf("Should return an error when given a wrong address")
	}
}

func TestCDCClient_Authenticate(t *testing.T) {
	ctx := context.Background()

	host, port := os.Getenv("MAXSCALE_HOST"), os.Getenv("MAXSCALE_PORT")
	addr := net.JoinHostPort(host, port)
	conn, err := maxscale.Connect(ctx, addr)
	if err != nil {
		t.Fatalf("Failed to connect to the MaxScale CDC listener at %s: %v", addr, err)
	}
	defer conn.Close()

	user, pwd := os.Getenv("MAXSCALE_USER"), os.Getenv("MAXSCALE_PASSWORD")
	err = conn.Authenticate(user, pwd)
	if err != nil {
		t.Fatalf("Could not authenticate to the MaxScale CDC listener with credentials: %s:%s: %v", user, pwd, err)
	}
}

func TestCDCClient_Authenticate_FailsWithWrongCredentials(t *testing.T) {
	ctx := context.Background()

	host, port := os.Getenv("MAXSCALE_HOST"), os.Getenv("MAXSCALE_PORT")
	addr := net.JoinHostPort(host, port)
	conn, err := maxscale.Connect(ctx, addr)
	if err != nil {
		t.Fatalf("Failed to connect to the MaxScale CDC listener at %s: %v", addr, err)
	}
	defer conn.Close()

	err = conn.Authenticate("wrong", "credentials")
	if err == nil {
		t.Fatalf("Should return an error when given wrong credentials")
	}
}

func TestCDCClient_Register(t *testing.T) {
	ctx := context.Background()

	host, port := os.Getenv("MAXSCALE_HOST"), os.Getenv("MAXSCALE_PORT")
	addr := net.JoinHostPort(host, port)
	conn, err := maxscale.Connect(ctx, addr)
	if err != nil {
		t.Fatalf("Failed to connect to the MaxScale CDC listener at %s: %v", addr, err)
	}
	defer conn.Close()

	user, pwd := os.Getenv("MAXSCALE_USER"), os.Getenv("MAXSCALE_PASSWORD")
	err = conn.Authenticate(user, pwd)
	if err != nil {
		t.Fatalf("Could not authenticate to the MaxScale CDC listener with credentials: %s:%s: %v", user, pwd, err)
	}

	uuid := uuid.NewString()
	err = conn.Register(uuid)
	if err != nil {
		t.Fatalf("Failed to register to the MaxScale CDC listener: %v", err)
	}
}

func TestCDCClient_Register_FailsIfEmptyIdentifier(t *testing.T) {
	ctx := context.Background()

	host, port := os.Getenv("MAXSCALE_HOST"), os.Getenv("MAXSCALE_PORT")
	addr := net.JoinHostPort(host, port)
	conn, err := maxscale.Connect(ctx, addr)
	if err != nil {
		t.Fatalf("Failed to connect to the MaxScale CDC listener at %s: %v", addr, err)
	}
	defer conn.Close()

	user, pwd := os.Getenv("MAXSCALE_USER"), os.Getenv("MAXSCALE_PASSWORD")
	err = conn.Authenticate(user, pwd)
	if err != nil {
		t.Fatalf("Could not authenticate to the MaxScale CDC listener with credentials: %s:%s: %v", user, pwd, err)
	}

	err = conn.Register("")
	if err == nil {
		t.Fatalf("Should return an error when trying to register with an empty identifier")
	}
}

func TestCDCClient_RequestData_ReturnsNoEventIfNonExistingTable(t *testing.T) {
	ctx := context.Background()

	host, port := os.Getenv("MAXSCALE_HOST"), os.Getenv("MAXSCALE_PORT")
	addr := net.JoinHostPort(host, port)
	conn, err := maxscale.Connect(ctx, addr)
	if err != nil {
		t.Fatalf("Failed to connect to the MaxScale CDC listener at %s: %v", addr, err)
	}
	defer conn.Close()

	user, pwd := os.Getenv("MAXSCALE_USER"), os.Getenv("MAXSCALE_PASSWORD")
	err = conn.Authenticate(user, pwd)
	if err != nil {
		t.Fatalf("Could not authenticate to the MaxScale CDC listener with credentials: %s:%s: %v", user, pwd, err)
	}

	uuid := uuid.NewString()
	err = conn.Register(uuid)
	if err != nil {
		t.Fatalf("Failed to register to the MaxScale CDC listener: %v", err)
	}

	data, err := conn.RequestData(ctx, "String", "bar")
	if err != nil {
		t.Fatalf("Could not request data to from table String.bar: %v\n", err)
	}

	for range data {
		t.Fatalf("Should not receive event if table does not exist\n")
	}
}

func TestCDCClient_RequestData(t *testing.T) {
	ctx := context.Background()

	host, port := os.Getenv("MAXSCALE_HOST"), os.Getenv("MAXSCALE_PORT")
	addr := net.JoinHostPort(host, port)
	conn, err := maxscale.Connect(ctx, addr)
	if err != nil {
		t.Fatalf("Failed to connect to the MaxScale CDC listener at %s: %v", addr, err)
	}
	defer conn.Close()

	user, pwd := os.Getenv("MAXSCALE_USER"), os.Getenv("MAXSCALE_PASSWORD")
	err = conn.Authenticate(user, pwd)
	if err != nil {
		t.Fatalf("Could not authenticate to the MaxScale CDC listener with credentials: %s:%s: %v", user, pwd, err)
	}

	uuid := uuid.NewString()
	err = conn.Register(uuid)
	if err != nil {
		t.Fatalf("Failed to register to the MaxScale CDC listener: %v", err)
	}

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	db, table := os.Getenv("MAXSCALE_DATABASE"), os.Getenv("MAXSCALE_TABLE")
	data, err := conn.RequestData(ctx, db, table)
	if err != nil {
		t.Fatalf("Could not request data to from table %s.%s: %v\n", db, table, err)
	}

	event := <-data
	expectedDDLEvent := &maxscale.DDLEvent{
		Namespace: "MaxScaleChangeDataSchema.avro",
		Type:      "record",
		Name:      "ChangeRecord",
		Table:     "tests",
		Database:  "test",
		Version:   1,
		GTID:      "0-3000-6",
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
					Type: "enum",
					Name: "EVENT_TYPES",
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
	ctx := context.Background()

	host, port := os.Getenv("MAXSCALE_HOST"), os.Getenv("MAXSCALE_PORT")
	addr := net.JoinHostPort(host, port)
	conn, err := maxscale.Connect(ctx, addr)
	if err != nil {
		t.Fatalf("Failed to connect to the MaxScale CDC listener at %s: %v", addr, err)
	}
	defer conn.Close()

	user, pwd := os.Getenv("MAXSCALE_USER"), os.Getenv("MAXSCALE_PASSWORD")
	err = conn.Authenticate(user, pwd)
	if err != nil {
		t.Fatalf("Could not authenticate to the MaxScale CDC listener with credentials: %s:%s: %v", user, pwd, err)
	}

	uuid := uuid.NewString()
	err = conn.Register(uuid)
	if err != nil {
		t.Fatalf("Failed to register to the MaxScale CDC listener: %v", err)
	}

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	db, table := os.Getenv("MAXSCALE_DATABASE"), os.Getenv("MAXSCALE_TABLE")
	data, err := conn.RequestData(ctx, db, table, maxscale.WithGTID("0-3000-8"))
	if err != nil {
		t.Fatalf("Could not request data to from table %s.%s: %v\n", db, table, err)
	}

	event := <-data
	expectedDDLEvent := &maxscale.DDLEvent{
		Namespace: "MaxScaleChangeDataSchema.avro",
		Type:      "record",
		Name:      "ChangeRecord",
		Table:     "tests",
		Database:  "test",
		Version:   1,
		GTID:      "0-3000-6",
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
					Type: "enum",
					Name: "EVENT_TYPES",
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
