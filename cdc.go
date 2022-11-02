package maxscale

import (
	"bytes"
	"context"
	"crypto/sha1"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net"
	"strings"
	"time"
)

const (
	defaultReadSize = 4096

	defaultDialTimeout  = time.Second * 5
	defaultReadTimeout  = time.Second * 5
	defaultWriteTimeout = time.Second * 5
)

type cdcConnectionOptions struct {
	readTimeout  time.Duration
	writeTimeout time.Duration
	dialTimeout  time.Duration
}

// CDCConnection represents a connection with a MaxScale CDC protocol listener.
type CDCConnection struct {
	conn    net.Conn
	options cdcConnectionOptions
}

// CDCConnectionOption is a function option used to parameterize a CDC connection.
type CDCConnectionOption func(*cdcConnectionOptions)

// WithDialTimeout sets the timeout of the dial call when creating the
// connection with the MaxScale protocol listener.
func WithDialTimeout(timeout time.Duration) CDCConnectionOption {
	return func(co *cdcConnectionOptions) {
		co.readTimeout = timeout
	}
}

// WithReadTimeout sets the timeout for all read calls over the connection
// with the MaxScale protocol listener.
func WithReadTimeout(timeout time.Duration) CDCConnectionOption {
	return func(co *cdcConnectionOptions) {
		co.readTimeout = timeout
	}
}

// WithReadTimeout sets the timeout for all write calls over the connection
// with the MaxScale protocol listener.
func WithWriteTimeout(timeout time.Duration) CDCConnectionOption {
	return func(co *cdcConnectionOptions) {
		co.readTimeout = timeout
	}
}

// Connect creates a new connection with the MaxScale CDC protocol listener
// listening at the given address.
//
// See: https://mariadb.com/kb/en/mariadb-maxscale-6-change-data-capture-cdc-protocol/#connection-and-authentication
func Connect(ctx context.Context, address string, opts ...CDCConnectionOption) (*CDCConnection, error) {
	cdcConn := &CDCConnection{
		options: cdcConnectionOptions{
			dialTimeout:  defaultDialTimeout,
			readTimeout:  defaultReadTimeout,
			writeTimeout: defaultWriteTimeout,
		},
	}

	for _, opt := range opts {
		opt(&cdcConn.options)
	}

	dialer := net.Dialer{
		Timeout: cdcConn.options.dialTimeout,
	}
	conn, err := dialer.DialContext(ctx, "tcp", address)
	if err != nil {
		return nil, fmt.Errorf("could not connect to %s over TCP: %w", address, err)
	}
	cdcConn.conn = conn

	return cdcConn, nil
}

// Authenticate sends an authentication message containing the given credentials
// to the MaxScale CDC protocol listener.
//
// See: https://mariadb.com/kb/en/mariadb-maxscale-6-change-data-capture-cdc-protocol/#connection-and-authentication
func (c *CDCConnection) Authenticate(user, password string) error {
	authMsg, err := c.formatAuthenticationMessage(user, password)
	if err != nil {
		return err
	}

	if err = c.conn.SetWriteDeadline(time.Now().Add(c.options.writeTimeout)); err != nil {
		return fmt.Errorf("could not set write deadline to the future write call on the connection: %w", err)
	}
	if _, err = c.conn.Write(authMsg); err != nil {
		return fmt.Errorf("could not write the authentication message to the connection: %w", err)
	}

	if err = c.conn.SetReadDeadline(time.Now().Add(c.options.readTimeout)); err != nil {
		return fmt.Errorf("could not set read deadline to the future read call on the connection: %w", err)
	}
	if err := c.checkResponse(); err != nil {
		return fmt.Errorf("failed to authenticate the user: %w", err)
	}

	return nil
}

// Register registers the connection with the given UUID.
//
// See: https://mariadb.com/kb/en/mariadb-maxscale-6-change-data-capture-cdc-protocol/#registration_1
func (c *CDCConnection) Register(uuid string) error {
	if err := c.conn.SetWriteDeadline(time.Now().Add(c.options.writeTimeout)); err != nil {
		return fmt.Errorf("could not set write deadline to the future write call on the connection: %w", err)
	}
	if _, err := c.conn.Write([]byte("REGISTER UUID=" + uuid + ", TYPE=JSON")); err != nil {
		return fmt.Errorf("could not write UUID %s to the connection: %w", uuid, err)
	}

	if err := c.conn.SetReadDeadline(time.Now().Add(c.options.readTimeout)); err != nil {
		return fmt.Errorf("could not set read deadline to the future write call on the connection: %w", err)
	}
	if err := c.checkResponse(); err != nil {
		return fmt.Errorf("failed to register the connection: %w", err)
	}

	return nil
}

// RequestDataOption is a functional option to parameterize a RequestData call.
type RequestDataOption func(*RequestDataOptions)

// WithVersion specifies the version of the table from which the event will be streamed.
func WithVersion(version string) RequestDataOption {
	return func(rdo *RequestDataOptions) {
		rdo.version = version
	}
}

// WithGTID specifies the GTID position where the events should start being streamed.
func WithGTID(gtid string) RequestDataOption {
	return func(rdo *RequestDataOptions) {
		rdo.gtid = gtid
	}
}

// RequestDataOptions contains the optional parameters of a RequestData call.
type RequestDataOptions struct {
	version string
	gtid    string
}

// RequestData starts fetching events from the given table in the given database.
//
// See: https://mariadb.com/kb/en/mariadb-maxscale-6-change-data-capture-cdc-protocol/#request-data
func (c *CDCConnection) RequestData(ctx context.Context, database, table string, opts ...RequestDataOption) (<-chan CDCEvent, error) {
	var options RequestDataOptions
	for _, opt := range opts {
		opt(&options)
	}

	dataStream := make(chan CDCEvent, 1)

	var requestDataCmd bytes.Buffer
	if _, err := requestDataCmd.WriteString("REQUEST-DATA " + database + "." + table); err != nil {
		return nil, fmt.Errorf("could not write the REQUEST-DATA command to the buffer: %w", err)
	}
	if options.version != "" {
		if _, err := requestDataCmd.WriteString("." + options.version); err != nil {
			return nil, fmt.Errorf("could not add the version to the REQUEST-DATA command in the buffer: %w", err)
		}
	}
	if options.gtid != "" {
		if _, err := requestDataCmd.WriteString(" " + options.gtid); err != nil {
			return nil, fmt.Errorf("could not add the GTID to the REQUEST-DATA command in the buffer: %w", err)
		}
	}

	if err := c.conn.SetWriteDeadline(time.Now().Add(c.options.writeTimeout)); err != nil {
		return nil, fmt.Errorf("could not set write deadline to the future write call on the connection: %w", err)
	}
	if _, err := c.conn.Write(requestDataCmd.Bytes()); err != nil {
		return nil, fmt.Errorf("could not write the REQUEST-DATA command to the connection: %w", err)
	}

	go func() {
		<-ctx.Done()
		c.Close()
	}()

	go func() {
		if err := c.startDecodingData(dataStream); err != nil {
			log.Printf("Failed to scan data: %v", err)
			close(dataStream)
		}
	}()

	return dataStream, nil
}

func (c *CDCConnection) startDecodingData(dataStream chan<- CDCEvent) error {
	if err := c.conn.SetReadDeadline(time.Time{}); err != nil {
		return fmt.Errorf("could not reset the read deadline on the connection: %w", err)
	}

	var readSchema bool
	dec := json.NewDecoder(c.conn)
	for {
		var data map[string]interface{}
		err := dec.Decode(&data)
		if err != nil {
			// The first read after requesting data from a database table should
			// read the table schema. However if the .avro file associated to the
			// table is not present in the specified avrodir on the filesystem
			// where MaxScale is running, an error is returned and should be read
			// over the TCP connection.
			if !readSchema {
				resp, err := readResponse(dec.Buffered())
				if err != nil {
					return fmt.Errorf("failed to read the .avro file missing error: %w", err)
				}
				return fmt.Errorf("failed to read the table schema: %s", resp)
			}
			return fmt.Errorf("failed to decode the CDC event: %w", err)
		}

		if !readSchema {
			readSchema = true
		}

		// Data has already been decoded through the JSON decoder therefore
		// there's no way something could go wrong while marshalling :)
		b, _ := json.Marshal(data)

		if _, ok := data["domain"]; ok {
			dmlEvent, err := c.decodeDMLEvent(b)
			if err != nil {
				return err
			}
			dataStream <- dmlEvent
		} else {
			ddlEvent, err := c.decodeDDLEvent(b)
			if err != nil {
				return err
			}
			dataStream <- ddlEvent
		}
	}
}

func (c *CDCConnection) decodeDMLEvent(data []byte) (*DMLEvent, error) {
	var event DMLEvent
	if err := json.Unmarshal(data, &event); err != nil {
		return nil, fmt.Errorf("failed to unmarshal the DML event data into a go value: %w", err)
	}

	// The table data are all the DML fields minus the fields that are not
	// columms of the table.
	if err := json.Unmarshal(data, &event.TableData); err != nil {
		return nil, fmt.Errorf("failed to unmarshal the table data into a go value: %w", err)
	}
	delete(event.TableData, "domain")
	delete(event.TableData, "server_id")
	delete(event.TableData, "sequence")
	delete(event.TableData, "event_number")
	delete(event.TableData, "timestamp")
	delete(event.TableData, "event_type")
	delete(event.TableData, "table_name")
	delete(event.TableData, "table_schema")

	return &event, nil
}

func (c *CDCConnection) decodeDDLEvent(data []byte) (*DDLEvent, error) {
	var event DDLEvent
	if err := json.Unmarshal(data, &event); err != nil {
		return nil, fmt.Errorf("failed to unmarshal the DDL event data into a go value: %w", err)
	}

	return &event, nil
}

func (c *CDCConnection) Close() error {
	return c.conn.Close()
}

func (c *CDCConnection) formatAuthenticationMessage(user, password string) ([]byte, error) {
	var buf bytes.Buffer

	_, err := buf.WriteString(user + ":")
	if err != nil {
		return nil, fmt.Errorf("could not write username in the authentication message: %w", err)
	}

	h := sha1.New()
	_, err = h.Write([]byte(password))
	if err != nil {
		return nil, fmt.Errorf("could not hash the user password: %w", err)
	}
	sha1Password := h.Sum(nil)
	_, err = buf.Write(sha1Password)
	if err != nil {
		return nil, fmt.Errorf("could not write password in the authentication message: %w", err)
	}

	authMsg := make([]byte, hex.EncodedLen(len(buf.Bytes())))
	_ = hex.Encode(authMsg, buf.Bytes())

	return authMsg, nil
}

func (c *CDCConnection) checkResponse() error {
	resp, err := readResponse(c.conn)
	if err != nil {
		return err
	}

	if isErrorResponse(resp) {
		return errors.New(resp)
	}

	return nil
}

func readResponse(r io.Reader) (string, error) {
	b := make([]byte, defaultReadSize)
	n, err := r.Read(b)
	if err != nil {
		return "", err
	}
	return string(b[:n]), nil
}

func isErrorResponse(resp string) bool {
	return strings.Contains(strings.ToLower(resp), "err")
}
