//go:build integration

package maxscale_test

import (
	"context"
	"net"
	"os"
	"reflect"
	"testing"

	"github.com/connectfit-team/maxscale-cdc-client"
	"github.com/google/uuid"
)

func TestCDCClient_Connect(t *testing.T) {
	ctx := context.Background()

	client := maxscale.NewCDCClient()
	defer client.Close()

	host, port := os.Getenv("MAXSCALE_HOST"), os.Getenv("MAXSCALE_PORT")
	addr := net.JoinHostPort(host, port)
	err := client.Connect(ctx, addr)
	if err != nil {
		t.Fatalf("Failed to connect to the MaxScale CDC listener at %s: %v", addr, err)
	}
}

func TestCDCClient_Connect_FailsIfWrongAddress(t *testing.T) {
	ctx := context.Background()

	client := maxscale.NewCDCClient()
	defer client.Close()

	err := client.Connect(ctx, "wrong address (:")
	if err == nil {
		t.Fatalf("Should return an error when given a wrong address")
	}
}

func TestCDCClient_Authenticate(t *testing.T) {
	ctx := context.Background()

	client := maxscale.NewCDCClient()
	defer client.Close()

	host, port := os.Getenv("MAXSCALE_HOST"), os.Getenv("MAXSCALE_PORT")
	addr := net.JoinHostPort(host, port)
	err := client.Connect(ctx, addr)
	if err != nil {
		t.Fatalf("Failed to connect to the MaxScale CDC listener at %s: %v", addr, err)
	}

	user, pwd := os.Getenv("MAXSCALE_USER"), os.Getenv("MAXSCALE_PASSWORD")
	err = client.Authenticate(user, pwd)
	if err != nil {
		t.Fatalf("Could not authenticate to the MaxScale CDC listener with credentials: %s:%s: %v", user, pwd, err)
	}
}

func TestCDCConnectionAuthenticate_FailsWithWrongCredentials(t *testing.T) {
	ctx := context.Background()

	client := maxscale.NewCDCClient()
	defer client.Close()

	host, port := os.Getenv("MAXSCALE_HOST"), os.Getenv("MAXSCALE_PORT")
	addr := net.JoinHostPort(host, port)
	err := client.Connect(ctx, addr)
	if err != nil {
		t.Fatalf("Failed to connect to the MaxScale CDC listener at %s: %v", addr, err)
	}

	err = client.Authenticate("wrong", "credentials")
	if err == nil {
		t.Fatalf("Should return an error when given wrong credentials")
	}
}

func TestCDCConnectionRegister(t *testing.T) {
	ctx := context.Background()

	client := maxscale.NewCDCClient()
	defer client.Close()

	host, port := os.Getenv("MAXSCALE_HOST"), os.Getenv("MAXSCALE_PORT")
	addr := net.JoinHostPort(host, port)
	err := client.Connect(ctx, addr)
	if err != nil {
		t.Fatalf("Failed to connect to the MaxScale CDC listener at %s: %v", addr, err)
	}

	user, pwd := os.Getenv("MAXSCALE_USER"), os.Getenv("MAXSCALE_PASSWORD")
	err = client.Authenticate(user, pwd)
	if err != nil {
		t.Fatalf("Could not authenticate to the MaxScale CDC listener with credentials: %s:%s: %v", user, pwd, err)
	}

	uuid := uuid.NewString()
	err = client.Register(uuid)
	if err != nil {
		t.Fatalf("Failed to register to the MaxScale CDC listener: %v", err)
	}
}

func TestCDCConnectionRegister_FailsIfEmptyIdentifier(t *testing.T) {
	ctx := context.Background()

	client := maxscale.NewCDCClient()
	defer client.Close()

	host, port := os.Getenv("MAXSCALE_HOST"), os.Getenv("MAXSCALE_PORT")
	addr := net.JoinHostPort(host, port)
	err := client.Connect(ctx, addr)
	if err != nil {
		t.Fatalf("Failed to connect to the MaxScale CDC listener at %s: %v", addr, err)
	}

	user, pwd := os.Getenv("MAXSCALE_USER"), os.Getenv("MAXSCALE_PASSWORD")
	err = client.Authenticate(user, pwd)
	if err != nil {
		t.Fatalf("Could not authenticate to the MaxScale CDC listener with credentials: %s:%s: %v", user, pwd, err)
	}

	err = client.Register("")
	if err == nil {
		t.Fatalf("Should return an error when trying to register with an empty identifier")
	}
}

func TestCDCConnectionRequestData_ReturnsNoEventIfNonExistingTable(t *testing.T) {
	ctx := context.Background()

	client := maxscale.NewCDCClient()
	defer client.Close()

	host, port := os.Getenv("MAXSCALE_HOST"), os.Getenv("MAXSCALE_PORT")
	addr := net.JoinHostPort(host, port)
	err := client.Connect(ctx, addr)
	if err != nil {
		t.Fatalf("Failed to connect to the MaxScale CDC listener at %s: %v", addr, err)
	}

	user, pwd := os.Getenv("MAXSCALE_USER"), os.Getenv("MAXSCALE_PASSWORD")
	err = client.Authenticate(user, pwd)
	if err != nil {
		t.Fatalf("Could not authenticate to the MaxScale CDC listener with credentials: %s:%s: %v", user, pwd, err)
	}

	uuid := uuid.NewString()
	err = client.Register(uuid)
	if err != nil {
		t.Fatalf("Failed to register to the MaxScale CDC listener: %v", err)
	}

	data, err := client.RequestData(ctx, "String", "bar")
	if err != nil {
		t.Fatalf("Could not request data to from table String.bar: %v\n", err)
	}

	for range data {
		t.Fatalf("Should not receive event if table does not exist\n")
	}
}

func TestCDCConnectionRequestData(t *testing.T) {
	ctx := context.Background()

	client := maxscale.NewCDCClient()
	defer client.Close()

	host, port := os.Getenv("MAXSCALE_HOST"), os.Getenv("MAXSCALE_PORT")
	addr := net.JoinHostPort(host, port)
	err := client.Connect(ctx, addr)
	if err != nil {
		t.Fatalf("Failed to connect to the MaxScale CDC listener at %s: %v", addr, err)
	}

	user, pwd := os.Getenv("MAXSCALE_USER"), os.Getenv("MAXSCALE_PASSWORD")
	err = client.Authenticate(user, pwd)
	if err != nil {
		t.Fatalf("Could not authenticate to the MaxScale CDC listener with credentials: %s:%s: %v", user, pwd, err)
	}

	uuid := uuid.NewString()
	err = client.Register(uuid)
	if err != nil {
		t.Fatalf("Failed to register to the MaxScale CDC listener: %v", err)
	}

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	db, table := os.Getenv("MAXSCALE_DATABASE"), os.Getenv("MAXSCALE_TABLE")
	data, err := client.RequestData(ctx, db, table)
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
	if !reflect.DeepEqual(event.(*maxscale.DDLEvent), expectedDDLEvent) {
		t.Fatalf("captured DDL event differs from the expected one")
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
	if !reflect.DeepEqual(dmlEvent, expectedDMLEvent) {
		t.Fatalf("captured DML event differs from the expected one")
	}
}

func TestCDCConnectionRequestData_WithGTID(t *testing.T) {
	ctx := context.Background()

	client := maxscale.NewCDCClient()
	defer client.Close()

	host, port := os.Getenv("MAXSCALE_HOST"), os.Getenv("MAXSCALE_PORT")
	addr := net.JoinHostPort(host, port)
	err := client.Connect(ctx, addr)
	if err != nil {
		t.Fatalf("Failed to connect to the MaxScale CDC listener at %s: %v", addr, err)
	}

	user, pwd := os.Getenv("MAXSCALE_USER"), os.Getenv("MAXSCALE_PASSWORD")
	err = client.Authenticate(user, pwd)
	if err != nil {
		t.Fatalf("Could not authenticate to the MaxScale CDC listener with credentials: %s:%s: %v", user, pwd, err)
	}

	uuid := uuid.NewString()
	err = client.Register(uuid)
	if err != nil {
		t.Fatalf("Failed to register to the MaxScale CDC listener: %v", err)
	}

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	db, table := os.Getenv("MAXSCALE_DATABASE"), os.Getenv("MAXSCALE_TABLE")
	data, err := client.RequestData(ctx, db, table, maxscale.WithGTID("0-3000-8"))
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
	if !reflect.DeepEqual(event.(*maxscale.DDLEvent), expectedDDLEvent) {
		t.Fatalf("captured DDL event differs from the expected one")
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
	if !reflect.DeepEqual(dmlEvent, expectedDMLEvent) {
		t.Fatalf("captured DML event differs from the expected one")
	}
}
