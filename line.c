//  Pick-n-Pack Line Controller, based on ZeroMQ's Paranoid Pirate queue

#include "czmq.h"
#define HEARTBEAT_LIVENESS  3       //  3-5 is reasonable. This determines when to decide a Module has gone offline
#define HEARTBEAT_INTERVAL  1000    //  msecs
#define INTERVAL_INIT       1000    //  Initial reconnect
#define INTERVAL_MAX       32000    //  After exponential backoff

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

typedef struct {
    char *name;
    zsock_t *frontend; // socket to frontend process, e.g. line controller
    zsock_t *backend; // socket to potential backend processes, e.g. submodules
    zsock_t *pipe; // socket to main loop
    size_t liveness; // liveness defines how many heartbeat failures are tolerable
    size_t interval; // interval defines at what interval heartbeats are sent
    uint64_t heartbeat_at; // heartbeat_at defines when to send next heartbeat
    zlist_t *modules;
} resource_t;

resource_t* creating(zsock_t *pipe, char *name) {
    printf("[%s] creating...", name);
    resource_t *self = (resource_t *) zmalloc (sizeof (resource_t));
    self->name = name;
    self->frontend = zsock_new_dealer("tcp://localhost:9001"); // TODO: this should be configured
    self->backend =  zsock_new_router ("tcp://*:9002"); // TODO: this should be configured
    self->pipe = pipe;
    self->modules = zlist_new ();
    printf("done.\n");
    return self;
}

int initializing(resource_t* self) {
    printf("[%s] initializing...", self->name);
    // send signal on pipe socket to acknowledge initialization
    zsock_signal (self->pipe, 0);

    zframe_t *frame = zframe_new (PPP_READY, 1);
    zframe_send (&frame, self->frontend, 0);

    printf("done.\n");

    return 0;

}

int configuring (resource_t* self) {
    printf("[%s] configuring...", self->name);
    //  If liveness hits zero, queue is considered disconnected
    self->liveness = HEARTBEAT_LIVENESS;
    self->interval = INTERVAL_INIT;

    //  Send out heartbeats at regular intervals
    self->heartbeat_at = zclock_time () + HEARTBEAT_INTERVAL;

    srandom ((unsigned) time (NULL));

    printf("done.\n");

    return 0;
};

int running(resource_t *self) {//  List of available modules
    //  Send out heartbeats at regular intervals
    uint64_t heartbeat_at = zclock_time () + HEARTBEAT_INTERVAL;

	zmq_pollitem_t items [] = {
		{ zsock_resolve(self->backend),  0, ZMQ_POLLIN, 0 },
		{ zsock_resolve(self->frontend), 0, ZMQ_POLLIN, 0 }
	};
	//  Poll frontend only if we have available modules
	int rc = zmq_poll (items, zlist_size (self->modules)? 2: 1,
		HEARTBEAT_INTERVAL * ZMQ_POLL_MSEC);
	if (rc == -1) {
		printf("E: Line Controller failed to poll sockets\n");
		return -1;              //  Interrupted
	}
	//  Handle module activity on backend
	if (items [0].revents & ZMQ_POLLIN) {
		//  Use module identity for load-balancing
		zmsg_t *msg = zmsg_recv (self->backend);
		if (!msg)
			return -1;          //  Interrupted

		//  Any sign of life from module means it's ready
		zframe_t *identity = zmsg_unwrap (msg);
		module_t *module = s_module_new (identity);
		s_module_ready (module, self->modules);

		//  Validate control message, or return reply to client
		if (zmsg_size (msg) == 1) {
			zframe_t *frame = zmsg_first (msg);
			if (memcmp (zframe_data (frame), PPP_READY, 1)
			&&  memcmp (zframe_data (frame), PPP_HEARTBEAT, 1)) {
				printf ("E: invalid message from module\n");
				zmsg_dump (msg);
			} else {
				printf("[%s] RX HB BACKEND %s\n", self->name, module->id_string);
			}
			zmsg_destroy (&msg);
		}
		else // we assume here all other messages are replies which need to be sent to the clients
			zmsg_send (&msg, self->frontend);
	}
	if (items [1].revents & ZMQ_POLLIN) {
		//  Poll frontend
		zmsg_t *msg = zmsg_recv (self->frontend);
		if (!msg)
			return -1;          //  Interrupted
		//  Validate control message, or return reply to client
		if (zmsg_size (msg) == 1)  {
			printf("[%s] RX HB FRONTEND\n", self->name);
			zframe_t *frame = zmsg_first (msg);
			if (memcmp (zframe_data (frame), PPP_READY, 1)
			&&  memcmp (zframe_data (frame), PPP_HEARTBEAT, 1)) {
				printf ("E: invalid message from module\n");
				zmsg_dump (msg);
			}
			zmsg_destroy (&msg);
		}
		else
			//  .split detecting a dead queue
			//  If the queue hasn't sent us heartbeats in a while, destroy the
			//  socket and reconnect. This is the simplest most brutal way of
			//  discarding any messages we might have sent in the meantime:
			if (--self->liveness == 0) {
				printf ("[%s] heartbeat failure, can't reach frontend\n", self->name);
				printf ("[%s] reconnecting in %zd msec...\n", self->name, self->interval);
				zclock_sleep (self->interval);

				if (self->interval < INTERVAL_MAX)
					self->interval *= 2;
				zsock_destroy(&self->frontend);
				self->frontend = zsock_new_dealer("tcp://localhost:9001"); // TODO: this should be configured.
				self->liveness = HEARTBEAT_LIVENESS;
			}
		//zframe_t *identity = s_modules_next (self->modules);
		//zmsg_prepend (msg, &identity);
		//zmsg_send (&msg, backend);
	}
	//  .split handle heartbeating
	//  We handle heartbeating after any socket activity. First, we send
	//  heartbeats to any idle modules if it's time. Then, we purge any
	//  dead modules:
	if (zclock_time () >= heartbeat_at) {
		module_t *module = (module_t *) zlist_first (self->modules);
		while (module) {
			zframe_send (&module->identity, self->backend,	ZFRAME_REUSE + ZFRAME_MORE);
			zframe_t *frame = zframe_new (PPP_HEARTBEAT, 1);
			zframe_send (&frame, self->backend, 0);
			printf("[%s] TX HB BACKEND %s\n", self->name, module->id_string);
			module = (module_t *) zlist_next (self->modules);
		}
		// Send heartbeat to frontend
		zframe_t *frame = zframe_new (PPP_HEARTBEAT, 1);
		zframe_send (&frame, self->frontend, 0);
		printf("[%s] TX HB FRONTEND\n", self->name);
		heartbeat_at = zclock_time () + HEARTBEAT_INTERVAL;
	}
	s_modules_purge (self->modules);

    return 0;
}


int pausing(resource_t *self) {
    printf("[%s] pausing...", self->name);
    printf("done.\n");
    return 0;
}
int finalizing(resource_t *self) {
    printf("[%s] finalizing...", self->name);
    //  When we're done, clean up properly
    while (zlist_size (self->modules)) {
    	module_t *module = (module_t *) zlist_pop (self->modules);
        s_module_destroy (&module);
    }
    zlist_destroy (&self->modules);

    zsock_destroy(&self->frontend);
    zsock_destroy(&self->backend);
    printf("done.\n");
    return 0;
}

int deleting(resource_t *self) {
    printf("[%s] deleting...", self->name);
    self->frontend = NULL;
    self->backend = NULL;
    // TODO: free allocated memory for resource_t
    printf("done.\n");
    return 0;
}

static void resource_actor(zsock_t *pipe, void *args)
{
    char* name = (char*) args;
    printf("[%s] actor started.\n", name);
    resource_t* self = creating(pipe, name);
    assert(self);
    initializing(self);
    configuring(self);
    while(!zsys_interrupted) {
	if(running(self) < 0) {
	    printf("[%s] running state interrupted!", name);
	    break;
	}
    }
    pausing(self);
    finalizing(self);
    deleting(self);
    printf("[%s] actor stopped.\n", name);
}

/* main */
int main(int argc, char** args)
{
    char* name = NULL;
    if(argc > 1)
        name = args[1];
    else
        name = "PnP Line";
    assert(name);

    // incoming data is handled by the actor thread
    zactor_t *actor = zactor_new (resource_actor, (void*)name);
    assert(actor);
    while(!zsys_interrupted) { sleep(1);};
    printf("[%s] main loop interrupted!\n", name);
    zactor_destroy(&actor);

    return 0;
}

