//go:build integration

package maxscale_test

import (
	"context"
	"net"
	"os"
	"testing"

	"github.com/connectfit-team/maxscale-cdc-client"
	"github.com/google/uuid"
)

func TestConnectCDC(t *testing.T) {
	ctx := context.Background()

	host, port := os.Getenv("MAXSCALE_HOST"), os.Getenv("MAXSCALE_PORT")
	addr := net.JoinHostPort(host, port)
	conn, err := maxscale.ConnectCDC(ctx, addr)
	if err != nil {
		t.Fatalf("Failed to connect to the MaxScale CDC listener at %s: %v", addr, err)
	}
	defer conn.Close()
}

func TestConnectCDC_FailsIfWrongAddress(t *testing.T) {
	ctx := context.Background()

	_, err := maxscale.ConnectCDC(ctx, "wrong address (:")
	if err == nil {
		t.Fatalf("Should return an error when given a wrong address")
	}
}

func TestCDCConnectionAuthenticate(t *testing.T) {
	ctx := context.Background()

	host, port := os.Getenv("MAXSCALE_HOST"), os.Getenv("MAXSCALE_PORT")
	addr := net.JoinHostPort(host, port)
	conn, err := maxscale.ConnectCDC(ctx, addr)
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

func TestCDCConnectionAuthenticate_FailsWithWrongCredentials(t *testing.T) {
	ctx := context.Background()

	host, port := os.Getenv("MAXSCALE_HOST"), os.Getenv("MAXSCALE_PORT")
	addr := net.JoinHostPort(host, port)
	conn, err := maxscale.ConnectCDC(ctx, addr)
	if err != nil {
		t.Fatalf("Failed to connect to the MaxScale CDC listener at %s: %v", addr, err)
	}
	defer conn.Close()

	err = conn.Authenticate("wrong", "credentials")
	if err == nil {
		t.Fatalf("Should return an error when given wrong credentials")
	}
}

func TestCDCConnectionRegister(t *testing.T) {
	ctx := context.Background()

	host, port := os.Getenv("MAXSCALE_HOST"), os.Getenv("MAXSCALE_PORT")
	addr := net.JoinHostPort(host, port)
	conn, err := maxscale.ConnectCDC(ctx, addr)
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

func TestCDCConnectionRegister_FailsIfEmptyIdentifier(t *testing.T) {
	ctx := context.Background()

	host, port := os.Getenv("MAXSCALE_HOST"), os.Getenv("MAXSCALE_PORT")
	addr := net.JoinHostPort(host, port)
	conn, err := maxscale.ConnectCDC(ctx, addr)
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

func TestCDCConnectionRequestData_ReturnsNoEventIfNonExistingTable(t *testing.T) {
	ctx := context.Background()

	host, port := os.Getenv("MAXSCALE_HOST"), os.Getenv("MAXSCALE_PORT")
	addr := net.JoinHostPort(host, port)
	conn, err := maxscale.ConnectCDC(ctx, addr)
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
		t.Fatalf("Failed to register to the MaxScale CDC listener: %v\n", err)
	}

	db, table := os.Getenv("MAXSCALE_DATABASE"), os.Getenv("MAXSCALE_TABLE")
	data, err := conn.RequestData(ctx, db, table)
	if err != nil {
		t.Fatalf("Could not request data to from table %s.%s: %v\n", db, table, err)
	}

	for range data {
		t.Fatalf("Should not receive event if table does not exist\n")
	}
}

// func TestCDCConnectionRequestData(t *testing.T) {
// 	ctx := context.Background()

// 	// TODO: Read configuration from env
// 	conn, err := maxscale.ConnectCDC(ctx, "maxscale:4001")
// 	if err != nil {
// 		t.Fatalf("Failed to connect to the MaxScale CDC listener: %v\n", err)
// 	}
// 	defer conn.Close()

// 	err = conn.Authenticate(, "maxpwd")
// 	if err != nil {
// 		t.Fatalf("Failed to authentication to the MaxScale CDC listener: %v\n", err)
// 	}

// 	uuid := uuid.NewString()
// 	err = conn.Register(uuid)
// 	if err != nil {
// 		t.Fatalf("Failed to register to the MaxScale CDC listener: %v\n", err)
// 	}

// 	ctx, cancel := context.WithCancel(ctx)
// 	defer cancel()
// 	db, table := "test", "game_states"
// 	data, err := conn.RequestData(ctx, db, table)
// 	if err != nil {
// 		t.Fatalf("Could not request data to from table %s.%s: %v\n", db, table, err)
// 	}

// 	event1 := <-data
// 	fmt.Println(event1)
// 	cancel()
// }
