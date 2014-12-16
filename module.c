//  Pick-n-Pack Module, based on ZeroMQ's Paranoid Pirate worker

#include "czmq.h"
#include "defs.h"

#define HEARTBEAT_LIVENESS  3       //  3-5 is reasonable
#define HEARTBEAT_INTERVAL  1000    //  msecs
#define INTERVAL_INIT       1000    //  Initial reconnect
#define INTERVAL_MAX       32000    //  After exponential backoff

//  Pick-n-Pack Protocol constants for signalling
#define PPP_READY       "\001"      //  Signals module is ready
#define PPP_HEARTBEAT   "\002"      //  Signals module heartbeat

typedef struct {
    zframe_t *identity;         //  Identity of Module
    char *id_string;            //  Printable identity
    int64_t expiry;             //  Expires at this time
} device_t;

//  Construct new device, i.e. new local object for Line Controller representing a device
static device_t *
s_device_new (zframe_t *identity)
{
    device_t *self = (device_t *) zmalloc (sizeof (device_t));
    self->identity = identity;
    self->id_string = zframe_strhex (identity);
    self->expiry = zclock_time ()
                 + HEARTBEAT_INTERVAL * HEARTBEAT_LIVENESS;
    return self;
}

//  Destroy specified device object, including identity frame.
static void
s_device_destroy (device_t **self_p)
{
    assert (self_p);
    if (*self_p) {
        device_t *self = *self_p;
        zframe_destroy (&self->identity);
        free (self->id_string);
        free (self);
        *self_p = NULL;
    }
}

//  The ready method puts a device to the end of the ready list:

static void
s_device_ready (device_t *self, zlist_t *devices)
{
    device_t *device = (device_t *) zlist_first (devices);
    while (device) {
        if (streq (self->id_string, device->id_string)) {
            zlist_remove (devices, device);
            s_device_destroy (&device);
            break;
        }
        device = (device_t *) zlist_next (devices);
    }
    zlist_append (devices, self);
}

//  The next method returns the next available device identity:
//  TODO: Not all devices have the same capabilities so we should not just pick any device...

static zframe_t *
s_devices_next (zlist_t *devices)
{
    device_t *device = zlist_pop (devices);
    assert (device);
    zframe_t *frame = device->identity;
    device->identity = NULL;
    s_device_destroy (&device);
    return frame;
}

//  The purge method looks for and kills expired devices. We hold devices
//  from oldest to most recent, so we stop at the first alive device:
//  TODO: if devices expire, it should be checked if this effects the working of the line!

static void
s_devices_purge (zlist_t *devices)
{
    device_t *device = (device_t *) zlist_first (devices);
    while (device) {
        if (zclock_time () < device->expiry)
            break;              //  device is alive, we're done here
	printf("I: Removing expired device %s\n", device->id_string);
        zlist_remove (devices, device);
        s_device_destroy (&device);
        device = (device_t *) zlist_first (devices);
    }
}

typedef struct {
    char *name;
    zsock_t *frontend; // socket to frontend process, e.g. line controller
    zsock_t *backend; // socket to potential backend processes, e.g. subdevices
    zsock_t *pipe; // socket to main loop
    size_t liveness; // liveness defines how many heartbeat failures are tolerable
    size_t interval; // interval defines at what interval heartbeats are sent
    uint64_t heartbeat_at; // heartbeat_at defines when to send next heartbeat
    zlist_t *devices;
} module_t;

module_t* creating(zsock_t *pipe, char *name) {
    printf("[Module %s] creating...", name);
    module_t *self = (module_t *) zmalloc (sizeof (module_t));
    self->name = name;
    self->frontend = zsock_new_dealer("tcp://localhost:9002"); // TODO: this should be configured
    self->backend = zsock_new_router ("tcp://*:9003");;
    self->pipe = pipe;
    self->devices =  zlist_new();
    printf("done.\n");
    return self;
}

int initializing(module_t* self) {
    printf("[Module %s] starting...", self->name);
    // send signal on pipe socket to acknowledge initialisation
    zsock_signal (self->pipe, 0);

    //  Tell frontend we're ready for work
    zframe_t *frame = zframe_new (self->name, sizeof(self->name));
    zframe_send (&frame, self->frontend, ZFRAME_MORE);
    frame = zframe_new(READY, 1);
    zframe_send (&frame, self->frontend, 0);
    printf("done.\n");

    return 0;

}

int configuring (module_t* self) {
    printf("[Module %s] configuring...", self->name);
    //  If liveness hits zero, queue is considered disconnected
    self->liveness = HEARTBEAT_LIVENESS;
    self->interval = INTERVAL_INIT;

    //  Send out heartbeats at regular intervals
    self->heartbeat_at = zclock_time () + HEARTBEAT_INTERVAL;

    srandom ((unsigned) time (NULL));

    printf("done.\n");

    return 0;
};

int running(module_t* self) {
	//  Send out heartbeats at regular intervals
	    uint64_t heartbeat_at = zclock_time () + HEARTBEAT_INTERVAL;

		zmq_pollitem_t items [] = {
			{ zsock_resolve(self->frontend), 0, ZMQ_POLLIN, 0 },
			{ zsock_resolve(self->backend),  0, ZMQ_POLLIN, 0 }
		};

		//  Poll backend only if we have available resources
		int rc = zmq_poll (items, zlist_size (self->devices)? 2: 1,
			HEARTBEAT_INTERVAL * ZMQ_POLL_MSEC);
		if (rc == -1) {
			printf("E: Line Controller failed to poll sockets\n");
			return -1;              //  Interrupted
		}

		//  Handle activity on backend
		if (items [1].revents & ZMQ_POLLIN) {
			//  Use module identity for load-balancing
			zmsg_t *msg = zmsg_recv (self->backend);
			if (!msg)
				return -1;          //  Interrupted

			//  Any sign of life from module means it's ready
			zframe_t *identity = zmsg_unwrap (msg);
			device_t *device = s_device_new (identity);
			s_device_ready (device, self->devices);

			//  Validate control message, or return reply to client
			if (zmsg_size (msg) == 1) {
				zframe_t *frame = zmsg_first (msg);
				if (memcmp (zframe_data (frame), PPP_READY, 1)
				&&  memcmp (zframe_data (frame), PPP_HEARTBEAT, 1)) {
					printf ("E: invalid message from device\n");
					zmsg_dump (msg);
				} else {
					printf("[%s] RX HB BACKEND %s\n", self->name, device->id_string);
				}
				zmsg_destroy (&msg);
			}
			else // we assume here all other messages are replies which need to be sent to the clients
				zmsg_send (&msg, self->frontend);
		}
		if (items [0].revents & ZMQ_POLLIN) {
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
					self->frontend = zsock_new_dealer("tcp://localhost:9002"); // TODO: this should be configured.
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
			device_t *device = (device_t *) zlist_first (self->devices);
			while (device) {
				zframe_send (&device->identity, self->backend,	ZFRAME_REUSE + ZFRAME_MORE);
				zframe_t *frame = zframe_new (PPP_HEARTBEAT, 1);
				zframe_send (&frame, self->backend, 0);
				printf("[%s] TX HB BACKEND %s\n", self->name, device->id_string);
				device = (device_t *) zlist_next (self->devices);
			}
			// Send heartbeat to frontend
			zframe_t *frame = zframe_new (PPP_HEARTBEAT, 1);
			zframe_send (&frame, self->frontend, 0);
			printf("[%s] TX HB FRONTEND\n", self->name);
			heartbeat_at = zclock_time () + HEARTBEAT_INTERVAL;
		}
		s_devices_purge (self->devices);

	    return 0;
}

int pausing(module_t *self) {
    printf("[Module %s] pausing...", self->name);
    printf("done.\n");
    return 0;
}
int finalizing(module_t *self) {
    printf("[Module %s] finalizing...", self->name);
    zsock_destroy(&self->frontend);
    zsock_destroy(&self->backend);
    printf("done.\n");
    return 0;
}

int deleting(module_t *self) {
    printf("[Module %s] deleting...", self->name);
    self->frontend = NULL;
    self->backend = NULL;
    printf("done.\n");
    return 0;
}

static void module_actor(zsock_t *pipe, void *args)
{ 
    char* name = (char*) args;
    printf("[Module %s] actor started.\n", name);
    module_t* module = creating(pipe, name);
    assert(module);
    initializing(module);    
    configuring(module);        
    while(!zsys_interrupted) {
	if(running(module) < 0) {
	    printf("[Module %s] running state interrupted!", name);
	    break;
	}
    }
    pausing(module);
    finalizing(module);
    deleting(module);
    printf("[Module %s] actor stopped.\n", name);
}
    
/* main */
int main(int argc, char** args)
{
    char* name = NULL;
    if(argc > 1)
        name = args[1];
    else 
        name = "R2D2";
    assert(name);

    // incoming data is handled by the actor thread
    zactor_t *actor = zactor_new (module_actor, (void*)name);
    assert(actor);
    while(!zsys_interrupted) { sleep(1);};
    printf("[MODULE %s] main loop interrupted!\n", name);
    zactor_destroy(&actor);

    return 0;
}
