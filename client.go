package cdc

import (
	"bufio"
	"bytes"
	"crypto/sha1"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"sync"
	"time"

	"golang.org/x/exp/slog"
)

var (
	// ErrNotConnected is returned when trying to stop the client from streaming
	// CDC events while it is not running.
	ErrNotConnected = errors.New("not connected")
)

var errPrefix = []byte("ERR")

var (
	defaultLogger = slog.Default()
)

const (
	defaultDialTimeout  = time.Second * 5
	defaultReadTimeout  = time.Second * 5
	defaultWriteTimeout = time.Second * 5
)

type clientOptions struct {
	dialTimeout  time.Duration
	readTimeout  time.Duration
	writeTimeout time.Duration
	logger       *slog.Logger
}

// ClientOption is a function option used to parameterize a CDC client.
type ClientOption func(*clientOptions)

// WithDialTimeout sets the timeout of the dial call when creating the
// connection with the MaxScale protocol listener.
func WithDialTimeout(timeout time.Duration) ClientOption {
	return func(co *clientOptions) {
		co.readTimeout = timeout
	}
}

// WithReadTimeout sets the timeout for all read calls over the connection.
func WithReadTimeout(timeout time.Duration) ClientOption {
	return func(co *clientOptions) {
		co.readTimeout = timeout
	}
}

// WithReadTimeout sets the timeout for all write calls over the connection.
func WithWriteTimeout(timeout time.Duration) ClientOption {
	return func(co *clientOptions) {
		co.readTimeout = timeout
	}
}

// WithLogger sets the logger used by the client.
func WithLogger(logger *slog.Logger) ClientOption {
	return func(co *clientOptions) {
		co.logger = logger
	}
}

// Client represents a MaxScale CDC client.
type Client struct {
	address  string
	user     string
	password string
	uuid     string
	conn     net.Conn
	wg       sync.WaitGroup
	options  clientOptions
}

// NewClient returns a new MaxScale CDC Client given an address to connect to,
// credentials to authenticate and an UUID to register the client.
func NewClient(address, user, password, uuid string, opts ...ClientOption) *Client {
	client := &Client{
		address:  address,
		user:     user,
		password: password,
		uuid:     uuid,
		options: clientOptions{
			dialTimeout:  defaultDialTimeout,
			readTimeout:  defaultReadTimeout,
			writeTimeout: defaultWriteTimeout,
			logger:       defaultLogger,
		},
	}

	for _, opt := range opts {
		opt(&client.options)
	}

	return client
}

// RequestDataOption is a functional option to parameterize a RequestData call.
type RequestDataOption func(*requestDataOptions)

// WithVersion specifies the version of the table from which the event will be streamed.
func WithVersion(version string) RequestDataOption {
	return func(rdo *requestDataOptions) {
		rdo.version = version
	}
}

// WithGTID specifies the GTID position where the events should start being streamed.
func WithGTID(gtid string) RequestDataOption {
	return func(rdo *requestDataOptions) {
		rdo.gtid = gtid
	}
}

type requestDataOptions struct {
	version string
	gtid    string
}

// RequestData starts fetching events from a given table and database.
// Optionally the version of the schema used and the GTID of the transaction
// from which the event will start being streamed can be set through options.
//
// If the schema file associated to the table is not present on the MaxScale
// filesystem no error is returned. Instead it logs an error and wait until it
// gets created to start streaming events.
//
// Call Stop to close the connection and release the associated ressources when done.
//
// Also see https://mariadb.com/kb/en/mariadb-maxscale-6-change-data-capture-cdc-protocol/#request-data
func (c *Client) RequestData(database, table string, opts ...RequestDataOption) (<-chan Event, error) {
	if err := c.connect(); err != nil {
		return nil, fmt.Errorf("failed to establish connection: %w", err)
	}

	if err := c.authenticate(); err != nil {
		return nil, fmt.Errorf("failed to authenticate: %w", err)
	}

	if err := c.register(); err != nil {
		return nil, fmt.Errorf("failed to register: %w", err)
	}

	var options requestDataOptions
	for _, opt := range opts {
		opt(&options)
	}
	return c.requestData(database, table, options.version, options.gtid)
}

// Stop closes the open connection if any and wait for all the ressources
// in use to be released.
func (c *Client) Stop() error {
	if c.conn == nil {
		return ErrNotConnected
	}

	err := c.conn.Close()

	c.wg.Wait()

	c.conn = nil
	return err
}

// See https://mariadb.com/kb/en/mariadb-maxscale-6-change-data-capture-cdc-protocol/#connection-and-authentication
func (c *Client) connect() error {
	dialer := &net.Dialer{
		Timeout: c.options.dialTimeout,
	}
	conn, err := dialer.Dial("tcp", c.address)
	if err != nil {
		return fmt.Errorf("could not connect to %s over TCP: %w", c.address, err)
	}
	c.conn = conn
	return nil
}

// See https://mariadb.com/kb/en/mariadb-maxscale-6-change-data-capture-cdc-protocol/#connection-and-authentication
func (c *Client) authenticate() error {
	authMsg, err := c.formatAuthenticationCommand(c.user, c.password)
	if err != nil {
		return fmt.Errorf("could not format the authentication command: %w", err)
	}

	if err = c.writeToConnection(authMsg); err != nil {
		return fmt.Errorf("could not write the authentication message to the connection: %w", err)
	}

	return c.checkResponse()
}

// See https://mariadb.com/kb/en/mariadb-maxscale-6-change-data-capture-cdc-protocol/#registration_1
func (c *Client) register() error {
	if err := c.writeToConnection([]byte("REGISTER UUID=" + c.uuid + ", TYPE=JSON")); err != nil {
		return fmt.Errorf("could not write UUID %s to the connection: %w", c.uuid, err)
	}
	return c.checkResponse()
}

// See https://mariadb.com/kb/en/mariadb-maxscale-6-change-data-capture-cdc-protocol/#request-data
func (c *Client) requestData(database, table, version, gtid string) (<-chan Event, error) {
	events := make(chan Event, 1)

	cmd, err := c.formatRequestDataCommand(database, table, version, gtid)
	if err != nil {
		return nil, fmt.Errorf("could not format the REQUEST-DATA command: %w", err)
	}

	if err := c.writeToConnection(cmd); err != nil {
		return nil, fmt.Errorf("could not write the REQUEST-DATA command to the connection: %w", err)
	}

	if err := c.conn.SetReadDeadline(time.Time{}); err != nil {
		return nil, fmt.Errorf("could not reset the read deadline on the connection: %w", err)
	}

	c.wg.Add(1)
	go func() {
		defer c.wg.Done()

		if err := c.handleEvents(events); err != nil {
			c.options.logger.Error("An error happened while decoding CDC events", err,
				"database", database,
				"table", table,
			)
		}
		close(events)
	}()

	return events, nil
}

func (c *Client) handleEvents(data chan<- Event) error {
	var readSchema bool
	scanner := bufio.NewScanner(c.conn)
	for scanner.Scan() {
		token := scanner.Bytes()

		// If the request for data is rejected, an error will be sent instead of the table schema.
		if !readSchema && isErrorResponse(token) {
			c.options.logger.Warn("Failed to read the table schema",
				"error", string(token),
			)
			continue
		}

		if !readSchema {
			readSchema = true
		}

		event, err := c.decodeEvent(token)
		if err != nil {
			return err
		}
		data <- event
	}
	return scanner.Err()
}

func (c *Client) decodeEvent(data []byte) (Event, error) {
	var (
		event Event
		err   error
	)
	if isDMLEvent(data) {
		if event, err = c.decodeDMLEvent(data); err != nil {
			return nil, fmt.Errorf("failed to decode DML event(%s): %w", data, err)
		}
	} else {
		if event, err = c.decodeDDLEvent(data); err != nil {
			return nil, fmt.Errorf("failed to decode DDL event(%s): %w", data, err)
		}
	}
	return event, nil
}

func (c *Client) decodeDMLEvent(data []byte) (*DMLEvent, error) {
	var event DMLEvent
	if err := json.Unmarshal(data, &event); err != nil {
		return nil, fmt.Errorf("failed to unmarshal the DML event data into a go value: %w", err)
	}
	event.Raw = make([]byte, len(data))
	copy(event.Raw, data)
	return &event, nil
}

func (c *Client) decodeDDLEvent(data []byte) (*DDLEvent, error) {
	var event DDLEvent
	if err := json.Unmarshal(data, &event); err != nil {
		return nil, fmt.Errorf("failed to unmarshal the DDL event data into a go value: %w", err)
	}
	return &event, nil
}

func (c *Client) formatAuthenticationCommand(user, password string) ([]byte, error) {
	var buf bytes.Buffer

	_, err := buf.WriteString(user + ":")
	if err != nil {
		return nil, fmt.Errorf("could not write username %q in the authentication message: %w", user, err)
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

func (c *Client) formatRequestDataCommand(database, table, version, gtid string) ([]byte, error) {
	var command bytes.Buffer

	if _, err := command.WriteString("REQUEST-DATA " + database + "." + table); err != nil {
		return nil, fmt.Errorf("could not write the REQUEST-DATA command to the buffer: %w", err)
	}

	if version != "" {
		if _, err := command.WriteString("." + version); err != nil {
			return nil, fmt.Errorf("could not add the version %q to the REQUEST-DATA command in the buffer: %w", version, err)
		}
	}

	if gtid != "" {
		if _, err := command.WriteString(" " + gtid); err != nil {
			return nil, fmt.Errorf("could not add the GTID %q to the REQUEST-DATA command in the buffer: %w", gtid, err)
		}
	}

	return command.Bytes(), nil
}

func (c *Client) writeToConnection(b []byte) error {
	if err := c.conn.SetWriteDeadline(time.Now().Add(c.options.writeTimeout)); err != nil {
		return fmt.Errorf("could not set write deadline to the future write call on the connection: %w", err)
	}

	if _, err := c.conn.Write(b); err != nil {
		return fmt.Errorf("could not write the authentication message to the connection: %w", err)
	}

	return nil
}

func (c *Client) readResponse() ([]byte, error) {
	if err := c.conn.SetReadDeadline(time.Now().Add(c.options.readTimeout)); err != nil {
		return nil, fmt.Errorf("could not set read deadline to the future read call on the connection: %w", err)
	}

	s := bufio.NewScanner(c.conn)
	s.Scan()
	return s.Bytes(), s.Err()
}

func (c *Client) checkResponse() error {
	resp, err := c.readResponse()
	if err != nil {
		return err
	}

	if isErrorResponse(resp) {
		return errors.New(string(resp))
	}

	return nil
}

func isErrorResponse(resp []byte) bool {
	return bytes.HasPrefix(resp, errPrefix)
}

func isDMLEvent(data []byte) bool {
	return bytes.HasPrefix(data, []byte(`{"domain":`))
}
