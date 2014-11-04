//  Pick-n-Pack Line Controller, based on ZeroMQ's Paranoid Pirate queue

#include "czmq.h"
#define HEARTBEAT_LIVENESS  3       //  3-5 is reasonable. This determines when to decide a Module has gone offline
#define HEARTBEAT_INTERVAL  1000    //  msecs

//  Pick-n-Pack Protocol constants for signalling
#define PPP_READY       "\001"      //  Signal used when Module has come online
#define PPP_HEARTBEAT   "\002"      //  Signals used for heartbeats between Line Controller and Modules

//  Here we define the Module class; a structure and a set of functions that
//  act as constructor, destructor, and methods on Module objects:

typedef struct {
    zframe_t *identity;         //  Identity of Module
    char *id_string;            //  Printable identity
    int64_t expiry;             //  Expires at this time
} module_t;

//  Construct new module, i.e. new local object for Line Controller representing a Module
static module_t *
s_module_new (zframe_t *identity)
{
    module_t *self = (module_t *) zmalloc (sizeof (module_t));
    self->identity = identity;
    self->id_string = zframe_strhex (identity);
    self->expiry = zclock_time ()
                 + HEARTBEAT_INTERVAL * HEARTBEAT_LIVENESS;
    return self;
}

//  Destroy specified module object, including identity frame.
static void
s_module_destroy (module_t **self_p)
{
    assert (self_p);
    if (*self_p) {
        module_t *self = *self_p;
        zframe_destroy (&self->identity);
        free (self->id_string);
        free (self);
        *self_p = NULL;
    }
}

//  The ready method puts a module to the end of the ready list:

static void
s_module_ready (module_t *self, zlist_t *modules)
{
    module_t *module = (module_t *) zlist_first (modules);
    while (module) {
        if (streq (self->id_string, module->id_string)) {
            zlist_remove (modules, module);
            s_module_destroy (&module);
            break;
        }
        module = (module_t *) zlist_next (modules);
    }
    zlist_append (modules, self);
}

//  The next method returns the next available module identity:
//  TODO: Not all Modules have the same capabilities so we should not just pick any Module...

static zframe_t *
s_modules_next (zlist_t *modules)
{
    module_t *module = zlist_pop (modules);
    assert (module);
    zframe_t *frame = module->identity;
    module->identity = NULL;
    s_module_destroy (&module);
    return frame;
}

//  The purge method looks for and kills expired modules. We hold modules
//  from oldest to most recent, so we stop at the first alive module:
//  TODO: if modules expire, it should be checked if this effects the working of the line!

static void
s_modules_purge (zlist_t *modules)
{
    module_t *module = (module_t *) zlist_first (modules);
    while (module) {
        if (zclock_time () < module->expiry)
            break;              //  module is alive, we're done here
	printf("I: Removing expired module %s\n", module->id_string);
        zlist_remove (modules, module);
        s_module_destroy (&module);
        module = (module_t *) zlist_first (modules);
    }
}

//  The main task of the Line Controller is to send tasks to the modules and exchange heartbeats with modules so we
//  can detect crashed or blocked module tasks:

int main (void)
{
    zsock_t *frontend = zsock_new_router ("tcp://*:5554");  //  TODO: this should be configured
    zsock_t *backend = zsock_new_router ("tcp://*:5555");   //  TODO: this should be configured

    //  List of available modules
    zlist_t *modules = zlist_new ();

    //  Send out heartbeats at regular intervals
    uint64_t heartbeat_at = zclock_time () + HEARTBEAT_INTERVAL;
    
    printf("I: Plant started\n");
    while (!zsys_interrupted) {
        zmq_pollitem_t items [] = {
            { zsock_resolve(backend),  0, ZMQ_POLLIN, 0 },
            { zsock_resolve(frontend), 0, ZMQ_POLLIN, 0 }
        };
        //  Poll frontend only if we have available modules
        int rc = zmq_poll (items, zlist_size (modules)? 2: 1,
            HEARTBEAT_INTERVAL * ZMQ_POLL_MSEC);
        if (rc == -1) {
            printf("E: Line Controller failed to poll sockets\n");
	    break;              //  Interrupted
	}
        //  Handle module activity on backend
        if (items [0].revents & ZMQ_POLLIN) {
            //  Use module identity for load-balancing
            zmsg_t *msg = zmsg_recv (backend);
            if (!msg)
                break;          //  Interrupted

            //  Any sign of life from module means it's ready
            zframe_t *identity = zmsg_unwrap (msg);
            module_t *module = s_module_new (identity);
            s_module_ready (module, modules);
	    
            //  Validate control message, or return reply to client
            if (zmsg_size (msg) == 1) {
                zframe_t *frame = zmsg_first (msg);
                if (memcmp (zframe_data (frame), PPP_READY, 1)
                &&  memcmp (zframe_data (frame), PPP_HEARTBEAT, 1)) {
                    printf ("E: invalid message from module\n");
                    zmsg_dump (msg);
                }
                zmsg_destroy (&msg);
            }
            else // we assume here all other messages are replies which need to be sent to the clients
                zmsg_send (&msg, frontend);
        }
        if (items [1].revents & ZMQ_POLLIN) {
            //  Now get next client request, route to next module
            zmsg_t *msg = zmsg_recv (frontend);
            if (!msg)
                break;          //  Interrupted
            zframe_t *identity = s_modules_next (modules); 
            zmsg_prepend (msg, &identity);
            zmsg_send (&msg, backend);
        }
        //  .split handle heartbeating
        //  We handle heartbeating after any socket activity. First, we send
        //  heartbeats to any idle modules if it's time. Then, we purge any
        //  dead modules:
        if (zclock_time () >= heartbeat_at) {
            module_t *module = (module_t *) zlist_first (modules);
            while (module) {
                zframe_send (&module->identity, backend,
                             ZFRAME_REUSE + ZFRAME_MORE);
                zframe_t *frame = zframe_new (PPP_HEARTBEAT, 1);
                zframe_send (&frame, backend, 0);
		printf("I: Sent heartbeat to line %s\n", module->id_string);
                module = (module_t *) zlist_next (modules);
            }
	    
            heartbeat_at = zclock_time () + HEARTBEAT_INTERVAL;
        }
        s_modules_purge (modules);
    }
    printf("I: Line Controller interrupted\n");
    //  When we're done, clean up properly
    while (zlist_size (modules)) {
        module_t *module = (module_t *) zlist_pop (modules);
        s_module_destroy (&module);
    }
    zlist_destroy (&modules);
    return 0;
}
