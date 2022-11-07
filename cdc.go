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
	"sync"
	"time"
)

const (
	defaultReadSize = 4096

	defaultDialTimeout  = time.Second * 5
	defaultReadTimeout  = time.Second * 5
	defaultWriteTimeout = time.Second * 5
)

type cdcClientOptions struct {
	readTimeout  time.Duration
	writeTimeout time.Duration
	dialTimeout  time.Duration
}

// CDCClientOption is a function option used to parameterize a CDC client.
type CDCClientOption func(*cdcClientOptions)

// WithDialTimeout sets the timeout of the dial call when creating the
// connection with the MaxScale protocol listener.
func WithDialTimeout(timeout time.Duration) CDCClientOption {
	return func(co *cdcClientOptions) {
		co.readTimeout = timeout
	}
}

// WithReadTimeout sets the timeout for all read calls over the connection
// with the MaxScale protocol listener.
func WithReadTimeout(timeout time.Duration) CDCClientOption {
	return func(co *cdcClientOptions) {
		co.readTimeout = timeout
	}
}

// WithReadTimeout sets the timeout for all write calls over the connection
// with the MaxScale protocol listener.
func WithWriteTimeout(timeout time.Duration) CDCClientOption {
	return func(co *cdcClientOptions) {
		co.readTimeout = timeout
	}
}

// CDCClient represents a connection with a MaxScale CDC protocol listener.
type CDCClient struct {
	address  string
	user     string
	password string
	uuid     string
	conn     net.Conn
	options  cdcClientOptions
	wg       sync.WaitGroup
}

// NewCDCClient returns a newly created CDCClient which will connect to the
// MaxScale CDC protocol listener at the given address, authenticate to it with
// the given credentials and register with uuid.
func NewCDCClient(address, user, password, uuid string, opts ...CDCClientOption) *CDCClient {
	c := &CDCClient{
		address:  address,
		user:     user,
		password: password,
		uuid:     uuid,
		options: cdcClientOptions{
			dialTimeout:  defaultDialTimeout,
			readTimeout:  defaultReadTimeout,
			writeTimeout: defaultWriteTimeout,
		},
	}

	for _, opt := range opts {
		opt(&c.options)
	}

	return c
}

// RequestData starts fetching events from the given table in the given database.
//
// See: https://mariadb.com/kb/en/mariadb-maxscale-6-change-data-capture-cdc-protocol/#request-data
func (c *CDCClient) RequestData(ctx context.Context, database, table string, opts ...RequestDataOption) (<-chan CDCEvent, error) {
	if err := c.connect(); err != nil {
		return nil, fmt.Errorf("failed to establish connection: %w", err)
	}

	if err := c.authenticate(); err != nil {
		return nil, fmt.Errorf("failed to authenticate: %w", err)
	}

	if err := c.register(); err != nil {
		return nil, fmt.Errorf("failed to register: %w", err)
	}

	var options RequestDataOptions
	for _, opt := range opts {
		opt(&options)
	}
	return c.requestData(ctx, database, table, options.version, options.gtid)
}

// connect creates a new connection with the MaxScale CDC protocol listener
// listening at the given address.
//
// See: https://mariadb.com/kb/en/mariadb-maxscale-6-change-data-capture-cdc-protocol/#connection-and-authentication
func (c *CDCClient) connect() error {
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

// authenticate sends an authentication message containing the given credentials
// to the MaxScale CDC protocol listener.
//
// See: https://mariadb.com/kb/en/mariadb-maxscale-6-change-data-capture-cdc-protocol/#connection-and-authentication
func (c *CDCClient) authenticate() error {
	authMsg, err := c.formatAuthenticationMessage(c.user, c.password)
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
	return c.checkResponse()
}

// register registers the connection with the given UUID.
//
// See: https://mariadb.com/kb/en/mariadb-maxscale-6-change-data-capture-cdc-protocol/#registration_1
func (c *CDCClient) register() error {
	if err := c.conn.SetWriteDeadline(time.Now().Add(c.options.writeTimeout)); err != nil {
		return fmt.Errorf("could not set write deadline to the future write call on the connection: %w", err)
	}
	if _, err := c.conn.Write([]byte("REGISTER UUID=" + c.uuid + ", TYPE=JSON")); err != nil {
		return fmt.Errorf("could not write UUID %s to the connection: %w", c.uuid, err)
	}

	if err := c.conn.SetReadDeadline(time.Now().Add(c.options.readTimeout)); err != nil {
		return fmt.Errorf("could not set read deadline to the future write call on the connection: %w", err)
	}

	return c.checkResponse()
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

func (c *CDCClient) requestData(ctx context.Context, database, table, version, gtid string) (<-chan CDCEvent, error) {
	dataStream := make(chan CDCEvent, 1)

	var requestDataCmd bytes.Buffer
	if _, err := requestDataCmd.WriteString("REQUEST-DATA " + database + "." + table); err != nil {
		return nil, fmt.Errorf("could not write the REQUEST-DATA command to the buffer: %w", err)
	}
	if version != "" {
		if _, err := requestDataCmd.WriteString("." + version); err != nil {
			return nil, fmt.Errorf("could not add the version to the REQUEST-DATA command in the buffer: %w", err)
		}
	}
	if gtid != "" {
		if _, err := requestDataCmd.WriteString(" " + gtid); err != nil {
			return nil, fmt.Errorf("could not add the GTID to the REQUEST-DATA command in the buffer: %w", err)
		}
	}

	if err := c.conn.SetWriteDeadline(time.Now().Add(c.options.writeTimeout)); err != nil {
		return nil, fmt.Errorf("could not set write deadline to the future write call on the connection: %w", err)
	}
	if _, err := c.conn.Write(requestDataCmd.Bytes()); err != nil {
		return nil, fmt.Errorf("could not write the REQUEST-DATA command to the connection: %w", err)
	}

	if err := c.conn.SetReadDeadline(time.Time{}); err != nil {
		return nil, fmt.Errorf("could not reset the read deadline on the connection: %w", err)
	}

	decodedEvents := make(chan CDCEvent, 1)

	c.wg.Add(1)
	go func() {
		defer c.wg.Done()

		if err := c.decodeEvents(decodedEvents); err != nil {
			log.Printf("An error happened while decoding CDC events: %v\n", err)
			close(decodedEvents)
			return
		}
		return
	}()

	c.wg.Add(1)
	go func() {
		defer c.wg.Done()

		for {
			select {
			case <-ctx.Done():
				c.Close()
				return
			case e, ok := <-decodedEvents:
				if !ok {
					close(dataStream)
					return
				}
				dataStream <- e
			}
		}
	}()

	return dataStream, nil
}

func (c *CDCClient) decodeEvents(dataStream chan<- CDCEvent) error {
	dec := json.NewDecoder(c.conn)
	for {
		var data map[string]interface{}
		err := dec.Decode(&data)
		if err != nil {
			// If the AVRO file associated to the table does not not exists an error
			// is raised by MaxScale. Since it is not a JSON error we have to read it
			// manually and then advance the offset to the next JSON object.
			var serr *json.SyntaxError
			if errors.As(err, &serr) {
				resp, err := readResponse(dec.Buffered())
				if err != nil {
					return fmt.Errorf("failed to read the .avro file missing error: %w", err)
				}
				log.Printf("Failed to read the table schema: %s", resp)

				// Create a new JSON decoder which starts reading after the error offset.
				dec = json.NewDecoder(c.conn)
				continue
			}
			return fmt.Errorf("failed to decode the CDC event: %w", err)
		}

		// Data has already been decoded through the JSON decoder therefore
		// there's no way something could go wrong while marshalling :)
		b, _ := json.Marshal(data)

		var event CDCEvent
		if _, ok := data["domain"]; ok {
			if event, err = c.decodeDMLEvent(b); err != nil {
				return fmt.Errorf("failed to decode DML event: %v\n", err)
			}
		} else {
			if event, err = c.decodeDDLEvent(b); err != nil {
				return fmt.Errorf("failed to decode DDL event: %v\n", err)
			}
		}
		dataStream <- event
	}
}

func (c *CDCClient) decodeDMLEvent(data []byte) (*DMLEvent, error) {
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

func (c *CDCClient) decodeDDLEvent(data []byte) (*DDLEvent, error) {
	var event DDLEvent
	if err := json.Unmarshal(data, &event); err != nil {
		return nil, fmt.Errorf("failed to unmarshal the DDL event data into a go value: %w", err)
	}

	return &event, nil
}

func (c *CDCClient) Close() error {
	if c.conn != nil {
		c.conn.Close()
	}
	c.wg.Wait()
	return nil
}

func (c *CDCClient) formatAuthenticationMessage(user, password string) ([]byte, error) {
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

func (c *CDCClient) checkResponse() error {
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
