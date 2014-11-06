//  Pick-n-Pack Plant Controller, based on ZeroMQ's Paranoid Pirate queue

#include "czmq.h"
#define HEARTBEAT_LIVENESS  3       //  3-5 is reasonable. This determines when to decide a line has gone offline
#define HEARTBEAT_INTERVAL  1000    //  msecs

//  Pick-n-Pack Protocol constants for signalling
#define PPP_READY       "\001"      //  Signal used when line has come online
#define PPP_HEARTBEAT   "\002"      //  Signals used for heartbeats between Plant and lines

//  Here we define the line class; a structure and a set of functions that
//  act as constructor, destructor, and methods on line objects:

typedef struct {
    zframe_t *identity;         //  Identity of line
    char *id_string;            //  Printable identity
    int64_t expiry;             //  Expires at this time
} line_t;

//  Construct new line, i.e. new local object for Plant representing a line
static line_t *
s_line_new (zframe_t *identity)
{
    line_t *self = (line_t *) zmalloc (sizeof (line_t));
    self->identity = identity;
    self->id_string = zframe_strhex (identity);
    self->expiry = zclock_time ()
                 + HEARTBEAT_INTERVAL * HEARTBEAT_LIVENESS;
    return self;
}

//  Destroy specified line object, including identity frame.
static void
s_line_destroy (line_t **self_p)
{
    assert (self_p);
    if (*self_p) {
        line_t *self = *self_p;
        zframe_destroy (&self->identity);
        free (self->id_string);
        free (self);
        *self_p = NULL;
    }
}

//  The ready method puts a line to the end of the ready list:

static void
s_line_ready (line_t *self, zlist_t *lines)
{
    line_t *line = (line_t *) zlist_first (lines);
    while (line) {
        if (streq (self->id_string, line->id_string)) {
            zlist_remove (lines, line);
            s_line_destroy (&line);
            break;
        }
        line = (line_t *) zlist_next (lines);
    }
    zlist_append (lines, self);
}

//  The next method returns the next available line identity:
//  TODO: Not all lines have the same capabilities so we should not just pick any line...

static zframe_t *
s_lines_next (zlist_t *lines)
{
    line_t *line = zlist_pop (lines);
    assert (line);
    zframe_t *frame = line->identity;
    line->identity = NULL;
    s_line_destroy (&line);
    return frame;
}

//  The purge method looks for and kills expired lines. We hold lines
//  from oldest to most recent, so we stop at the first alive line:
//  TODO: if lines expire, it should be checked if this effects the working of the line!

static void
s_lines_purge (zlist_t *lines)
{
    line_t *line = (line_t *) zlist_first (lines);
    while (line) {
        if (zclock_time () < line->expiry)
            break;              //  line is alive, we're done here
	printf("I: Removing expired line %s\n", line->id_string);
        zlist_remove (lines, line);
        s_line_destroy (&line);
        line = (line_t *) zlist_first (lines);
    }
}

//  The main task of the Plant is to send tasks to the lines and exchange heartbeats with lines so we
//  can detect crashed or blocked line tasks:

int main (void)
{
	char* name = "PnP Plant";
    zsock_t *frontend = zsock_new_router ("tcp://*:9000");  //  TODO: this should be configured
    zsock_t *backend = zsock_new_router ("tcp://*:9001");   //  TODO: this should be configured

    //  List of available lines
    zlist_t *lines = zlist_new ();

    //  Send out heartbeats at regular intervals
    uint64_t heartbeat_at = zclock_time () + HEARTBEAT_INTERVAL;
    
    printf("[%s] started\n", name);
    while (!zsys_interrupted) {
        zmq_pollitem_t items [] = {
            { zsock_resolve(backend),  0, ZMQ_POLLIN, 0 },
            { zsock_resolve(frontend), 0, ZMQ_POLLIN, 0 }
        };
        //  Poll frontend only if we have available lines
        int rc = zmq_poll (items, zlist_size (lines)? 2: 1,
            HEARTBEAT_INTERVAL * ZMQ_POLL_MSEC);
        if (rc == -1) {
            printf("E: Plant failed to poll sockets\n");
	    break;              //  Interrupted
	}
        //  Handle line activity on backend
        if (items [0].revents & ZMQ_POLLIN) {
            //  Use line identity for load-balancing
            zmsg_t *msg = zmsg_recv (backend);
            if (!msg)
                break;          //  Interrupted

            //  Any sign of life from line means it's ready
            zframe_t *identity = zmsg_unwrap (msg);
            line_t *line = s_line_new (identity);
            s_line_ready (line, lines);
	    
            //  Validate control message, or return reply to client
            if (zmsg_size (msg) == 1) {
                zframe_t *frame = zmsg_first (msg);
                if (memcmp (zframe_data (frame), PPP_READY, 1)
                &&  memcmp (zframe_data (frame), PPP_HEARTBEAT, 1)) {
                    printf ("E: invalid message from line\n");
                    zmsg_dump (msg);
                } else {
                	printf("[%s] RX HB BACKEND %s\n", name, line->id_string);
                }
                zmsg_destroy (&msg);
            }
            else // we assume here all other messages are replies which need to be sent to the clients
                zmsg_send (&msg, frontend);
        }
        if (items [1].revents & ZMQ_POLLIN) {
            //  Now get next client request, route to next line
            zmsg_t *msg = zmsg_recv (frontend);
            if (!msg)
                break;          //  Interrupted
            zframe_t *identity = s_lines_next (lines); 
            zmsg_prepend (msg, &identity);
            zmsg_send (&msg, backend);
        }
        //  .split handle heartbeating
        //  We handle heartbeating after any socket activity. First, we send
        //  heartbeats to any idle lines if it's time. Then, we purge any
        //  dead lines:
        if (zclock_time () >= heartbeat_at) {
            line_t *line = (line_t *) zlist_first (lines);
            while (line) {
                zframe_send (&line->identity, backend,
                             ZFRAME_REUSE + ZFRAME_MORE);
                zframe_t *frame = zframe_new (PPP_HEARTBEAT, 1);
                zframe_send (&frame, backend, 0);
                printf("[%s] TX HB BACKEND %s\n", name, line->id_string);
                line = (line_t *) zlist_next (lines);
            }
	    
            heartbeat_at = zclock_time () + HEARTBEAT_INTERVAL;
        }
        s_lines_purge (lines);
    }
    printf("I: Plant interrupted\n");
    //  When we're done, clean up properly
    while (zlist_size (lines)) {
        line_t *line = (line_t *) zlist_pop (lines);
        s_line_destroy (&line);
    }
    zlist_destroy (&lines);
    return 0;
}
