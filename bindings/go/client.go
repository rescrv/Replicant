package replicant

// #cgo pkg-config: replicant
// #include <stdlib.h>
// #include <string.h>
// #include <poll.h>
// #include <replicant.h>
import "C"
import "unsafe"
import "fmt"
import "runtime"
import "sync"

const (
	SUCCESS        = C.REPLICANT_SUCCESS
	MAYBE          = C.REPLICANT_MAYBE
	SEE_ERRNO      = C.REPLICANT_SEE_ERRNO
	CLUSTER_JUMP   = C.REPLICANT_CLUSTER_JUMP
	COMM_FAILED    = C.REPLICANT_COMM_FAILED
	OBJ_NOT_FOUND  = C.REPLICANT_OBJ_NOT_FOUND
	OBJ_EXIST      = C.REPLICANT_OBJ_EXIST
	FUNC_NOT_FOUND = C.REPLICANT_FUNC_NOT_FOUND
	COND_NOT_FOUND = C.REPLICANT_COND_NOT_FOUND
	COND_DESTROYED = C.REPLICANT_COND_DESTROYED
	SERVER_ERROR   = C.REPLICANT_SERVER_ERROR
	TIMEOUT        = C.REPLICANT_TIMEOUT
	INTERRUPTED    = C.REPLICANT_INTERRUPTED
	NONE_PENDING   = C.REPLICANT_NONE_PENDING
	INTERNAL       = C.REPLICANT_INTERNAL
	EXCEPTION      = C.REPLICANT_EXCEPTION
	GARBAGE        = C.REPLICANT_GARBAGE
)

type Status int

type Error struct {
	Status   Status
	Message  string
	Location string
}

func (s Status) String() string {
	return C.GoString(C.replicant_returncode_to_string(C.enum_replicant_returncode(s)))
}

func (e Error) Error() string {
	return C.GoString(C.replicant_returncode_to_string(C.enum_replicant_returncode(e.Status))) + ": " + e.Message
}

func (e Error) String() string {
	return e.Error()
}

type Client struct {
	mutex     sync.Mutex
	ptr       *C.struct_replicant_client
	errChan   chan Error
	closeChan chan bool
	ops       map[int64]chan Error
}

func NewClient(connection string) (*Client, error, chan Error) {
    conn := C.CString(connection)
    defer C.free(unsafe.Pointer(conn))
	ptr := C.replicant_client_create_conn_str(conn)
    if ptr == nil {
        return nil, fmt.Errorf("could not create replicant client"), nil
    }
	errChan := make(chan Error, 16)
	closeChan := make(chan bool, 1)
    client := &Client{sync.Mutex{}, ptr, errChan, closeChan, map[int64]chan Error{}}
    runtime.SetFinalizer(client, clientFinalizer)
    go client.runForever()
    return client, nil, errChan
}

func (client *Client) Destroy() {
    close(client.closeChan)
	close(client.errChan)
}

func clientFinalizer(c *Client) {
	c.mutex.Lock()
	defer c.mutex.Unlock()
	C.replicant_client_destroy(c.ptr)
	for _, val := range c.ops {
		close(val)
	}
}

func (client *Client) runForever() {
	for {
		select {
		case <-client.closeChan:
			return
		default:
			C.replicant_client_block(client.ptr, 250)
			var loop_status C.enum_replicant_returncode
			client.mutex.Lock()
			reqid := int64(C.replicant_client_loop(client.ptr, 0, &loop_status))
			if reqid < 0 && loop_status == TIMEOUT {
				// pass
			} else if reqid < 0 && loop_status == NONE_PENDING {
				// pass
			} else if reqid < 0 && loop_status == INTERRUPTED {
				// pass
			} else if reqid < 0 {
				e := Error{Status(loop_status),
					C.GoString(C.replicant_client_error_message(client.ptr)),
					C.GoString(C.replicant_client_error_location(client.ptr))}
				client.errChan <- e
			} else if c, ok := (client.ops)[reqid]; ok {
				e := Error{Status(loop_status),
					C.GoString(C.replicant_client_error_message(client.ptr)),
					C.GoString(C.replicant_client_error_location(client.ptr))}
				c <- e
				delete(client.ops, reqid)
			}
			client.mutex.Unlock()
		}
	}
}

func (client *Client) Poke() (err *Error) {
    err = nil
	var c_status C.enum_replicant_returncode
	done := make(chan Error)
	client.mutex.Lock()
    reqid := C.replicant_client_poke(client.ptr, &c_status)
    if reqid >= 0 {
        client.ops[int64(reqid)] = done
    } else {
	    err = &Error{Status(c_status),
	             C.GoString(C.replicant_client_error_message(client.ptr)),
	             C.GoString(C.replicant_client_error_location(client.ptr))}
    }
    client.mutex.Unlock()
    if reqid >= 0 {
        rc := <-done
        if c_status != SUCCESS {
            err = &rc
            err.Status = Status(c_status)
        }
    }
    return
}

func (client *Client) GenerateUniqueNumber() (num uint64, err *Error) {
    err = nil
	var c_status C.enum_replicant_returncode
    var c_num C.uint64_t
	done := make(chan Error)
	client.mutex.Lock()
    reqid := C.replicant_client_generate_unique_number(client.ptr, &c_status, &c_num)
    if reqid >= 0 {
        client.ops[int64(reqid)] = done
    } else {
	    err = &Error{Status(c_status),
	             C.GoString(C.replicant_client_error_message(client.ptr)),
	             C.GoString(C.replicant_client_error_location(client.ptr))}
    }
    client.mutex.Unlock()
    if reqid >= 0 {
        rc := <-done
        num = uint64(c_num)
        if c_status != SUCCESS {
            err = &rc
            err.Status = Status(c_status)
        }
    }
    return
}

const (
    CALL_IDEMPOTENT = C.REPLICANT_CALL_IDEMPOTENT
    CALL_ROBUST     = C.REPLICANT_CALL_ROBUST
)

func (client *Client) Call(obj string, fun string, input []byte, flags uint) (output []byte, err *Error) {
    output = nil
    err = &Error{SUCCESS, "success", ""}
    c_obj := C.CString(obj)
    defer C.free(unsafe.Pointer(c_obj))
    c_func := C.CString(fun)
    defer C.free(unsafe.Pointer(c_func))
    c_flags := C.unsigned(flags)
	var c_status C.enum_replicant_returncode
    var c_output *C.char = nil
    var c_output_sz C.size_t = 0
	done := make(chan Error)
	client.mutex.Lock()
    reqid := C.replicant_client_call(client.ptr, c_obj, c_func,
                                     (*C.char)(unsafe.Pointer(&input[0])), C.size_t(len(input)),
                                     c_flags, &c_status, &c_output, &c_output_sz)
    if reqid >= 0 {
        client.ops[int64(reqid)] = done
    } else {
	    err = &Error{Status(c_status),
	             C.GoString(C.replicant_client_error_message(client.ptr)),
	             C.GoString(C.replicant_client_error_location(client.ptr))}
    }
    client.mutex.Unlock()
    if reqid >= 0 {
        rc := <-done
        if c_status == SUCCESS {
            if c_output_sz > 0 {
                output = C.GoBytes(unsafe.Pointer(c_output), C.int(c_output_sz))
            } else {
                output = make([]byte, 0)
            }
        } else {
            err = &rc
            err.Status = Status(c_status)
        }
    }
    return
}

// XXX
//int64_t
//replicant_client_cond_wait(struct replicant_client* client,
//                           const char* object,
//                           const char* cond,
//                           uint64_t state,
//                           enum replicant_returncode* status,
//                           char** data, size_t* data_sz);
//
//int64_t
//replicant_client_cond_follow(struct replicant_client* client,
//                             const char* object,
//                             const char* cond,
//                             enum replicant_returncode* status,
//                             uint64_t* state,
//                             char** data, size_t* data_sz);
//
//int64_t
//replicant_client_defended_call(struct replicant_client* client,
//                               const char* object,
//                               const char* enter_func,
//                               const char* enter_input, size_t enter_input_sz,
//                               const char* exit_func,
//                               const char* exit_input, size_t exit_input_sz,
//                               enum replicant_returncode* status);
