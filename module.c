//  Pick-n-Pack Module, based on ZeroMQ's Paranoid Pirate worker

#include "czmq.h"
#include "defs.h"

resource_t* creating(resource_t *self, zsock_t *pipe, char *name) {
    printf("[%s] creating...", name);
    self->name = name;
    self->frontend = zsock_new_dealer("tcp://localhost:9002"); // TODO: this should be configured
    self->backend =  zsock_new_router ("tcp://*:9003"); // TODO: this should be configured
    self->pipe = pipe;
    self->backend_resources = zlist_new ();
    self->required_resources = zlist_new();
    printf("...done.\n");
    return self;
}

int initializing(resource_t* self) {
	printf("[%s] initializing...", self->name);
    // send signal on pipe socket to acknowledge initialisation
    zsock_signal (self->pipe, 0);

    //  Tell frontend we're ready for work
    zframe_t *frame = zframe_new (PNP_QAS_ID, sizeof(PNP_QAS_ID));
    zframe_send (&frame, self->frontend, ZFRAME_MORE);
    frame = zframe_new(READY, sizeof(READY));
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

int running(resource_t* self) {
	//  Send out heartbeats at regular intervals
	    uint64_t heartbeat_at = zclock_time () + HEARTBEAT_INTERVAL;

		zmq_pollitem_t items [] = {
			{ zsock_resolve(self->frontend), 0, ZMQ_POLLIN, 0 },
			{ zsock_resolve(self->backend),  0, ZMQ_POLLIN, 0 }
		};

		//  Poll backend only if we have available resources
		int rc = zmq_poll (items, zlist_size (self->backend_resources)? 2: 1,
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
			//backend_resource_t *backend_resource = s_backend_resource_new (identity);
			//s_backend_resource_ready (backend_resource, self->backend_resources);

			//  Validate control message, or return reply to client
			if (zmsg_size (msg) == 2) {
				// ID
				zframe_t *frame = zmsg_first (msg);
				printf("[%s] RX MSG FROM %s\n", self->name, uuid_to_name(zframe_data(frame)));
				backend_resource_t *backend_resource = s_backend_resource_new (identity, zframe_data(frame));
				frame = zmsg_next(msg);
				if (memcmp (zframe_data (frame), READY, 0)) {
					printf("[%s] RX READY BACKEND %s\n", self->name, backend_resource->id_string);
					s_backend_resource_ready (backend_resource, self->backend_resources);
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
				if (memcmp (zframe_data (frame), READY, 1)
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
			backend_resource_t *backend_resource = (backend_resource_t *) zlist_first (self->backend_resources);
			while (backend_resource) {
				zframe_send (&backend_resource->identity, self->backend,	ZFRAME_REUSE + ZFRAME_MORE);
				zframe_t *frame = zframe_new (PPP_HEARTBEAT, 1);
				zframe_send (&frame, self->backend, 0);
				printf("[%s] TX HB BACKEND %s\n", self->name, backend_resource->id_string);
				backend_resource = (backend_resource_t *) zlist_next (self->backend_resources);
			}
			// Send status as heartbeat to frontend
			zframe_t *frame = zframe_new (PNP_QAS_ID, sizeof(PNP_QAS_ID));
			zframe_send (&frame, self->frontend, ZFRAME_MORE);
			frame = zframe_new(RUNNING, sizeof(RUNNING));
			zframe_send (&frame, self->frontend, ZFRAME_MORE);
			frame = zframe_new(RUN, sizeof(RUN));
			zframe_send (&frame, self->frontend, 0);
			printf("[%s] TX HB FRONTEND\n", self->name);
			heartbeat_at = zclock_time () + HEARTBEAT_INTERVAL;
		}
		s_backend_resources_purge (self->backend_resources);

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
	while (zlist_size (self->backend_resources)) {
		backend_resource_t *backend_resource = (backend_resource_t *) zlist_pop (self->backend_resources);
		s_backend_resource_destroy (&backend_resource);
	}
	zlist_destroy (&self->backend_resources);

	zsock_destroy(&self->frontend);
	zsock_destroy(&self->backend);
	printf("done.\n");
	return 0;
}

int deleting(resource_t *self) {
	printf("[%s] deleting...", self->name);
	self->frontend = NULL;
	self->backend = NULL;
	printf("done.\n");
	return 0;
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
    zactor_t *actor = zactor_new (resource_actor, (void*)name);
    assert(actor);
    while(!zsys_interrupted) { sleep(1);};
    printf("[MODULE %s] main loop interrupted!\n", name);
    zactor_destroy(&actor);

    return 0;
}
