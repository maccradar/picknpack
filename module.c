//  Pick-n-Pack Module, based on ZeroMQ's Paranoid Pirate worker

#include "czmq.h"
#define HEARTBEAT_LIVENESS  3       //  3-5 is reasonable
#define HEARTBEAT_INTERVAL  1000    //  msecs
#define INTERVAL_INIT       1000    //  Initial reconnect
#define INTERVAL_MAX       32000    //  After exponential backoff

//  Pick-n-Pack Protocol constants for signalling
#define PPP_READY       "\001"      //  Signals module is ready
#define PPP_HEARTBEAT   "\002"      //  Signals module heartbeat

//  Helper function that returns a new configured socket
//  connected to the Paranoid Pirate queue

static zsock_t *
s_module_socket (char *name) {
    zsock_t *module = zsock_new_dealer("tcp://localhost:5556"); // TODO: this should be configured

    //  Tell Line Controller we're ready for work
    printf ("[Module %s] socket ready\n", name);
    zframe_t *frame = zframe_new (PPP_READY, 1);
    zframe_send (&frame, module, 0);

    return module;
}

typedef struct {
    char *name;
    zsock_t *frontend; // socket to frontend process, e.g. line controller
    zsock_t *backend; // socket to potential backend processes, e.g. submodules
    zsock_t *pipe; // socket to main loop
    size_t liveness; // liveness defines how many heartbeat failures are tolerable
    size_t interval; // interval defines at what interval heartbeats are sent
    uint64_t heartbeat_at; // heartbeat_at defines when to send next heartbeat
} module_t;

module_t* creating(zsock_t *pipe, char *name) {
    printf("[Module %s] creating...", name);
    module_t *self = (module_t *) zmalloc (sizeof (module_t));
    self->name = name;
    self->frontend = s_module_socket(name);
    self->backend = NULL;
    self->pipe = pipe;
    printf("done.\n");
    return self;
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

int initializing(module_t* self) {
    printf("[Module %s] starting...", self->name);
    // send signal on pipe socket to acknowledge initialisation
    zsock_signal (self->pipe, 0);
    printf("done.\n");

    return 0;

}

int running(module_t* self) {
    zmq_pollitem_t items [] = { { zsock_resolve(self->frontend),  0, ZMQ_POLLIN, 0 } };
    int rc = zmq_poll (items, 1, self->interval * ZMQ_POLL_MSEC);
    if (rc == -1)
        return -1; //  Interrupted

    if (items [0].revents & ZMQ_POLLIN) {
        //  Get message
        //  - 2-part envelope + content -> request
        //  - 1-part HEARTBEAT -> heartbeat
        zmsg_t *msg = zmsg_recv (self->frontend);
        if (!msg)
            return -1; //  Interrupted

        if (zmsg_size (msg) == 2) {
            printf ("I: normal reply\n");
            zmsg_print(msg);
            zmsg_send (&msg, self->frontend);
            self->liveness = HEARTBEAT_LIVENESS;
            sleep (1);              //  Do some heavy work
            if (zsys_interrupted)
                return -1;
            }
            else
            //  .split handle heartbeats
            //  When we get a heartbeat message from the queue, it means the
            //  queue was (recently) alive, so we must reset our liveness
            //  indicator:
            if (zmsg_size (msg) == 1) {
                zframe_t *frame = zmsg_first (msg);
                if (memcmp (zframe_data (frame), PPP_HEARTBEAT, 1) == 0)
                    self->liveness = HEARTBEAT_LIVENESS;
                else {
                    printf ("E: invalid message\n");
                    zmsg_dump (msg);
                }
                zmsg_destroy (&msg);
            }
            else {
                printf ("E: invalid message\n");
                zmsg_dump (msg);
            }
            self->interval = INTERVAL_INIT;
        }
        else
        //  .split detecting a dead queue
        //  If the queue hasn't sent us heartbeats in a while, destroy the
        //  socket and reconnect. This is the simplest most brutal way of
        //  discarding any messages we might have sent in the meantime:
        if (--self->liveness == 0) {
            printf ("[Module %s] heartbeat failure, can't reach frontend\n", self->name);
            printf ("[Module %s] reconnecting in %zd msec...\n", self->name, self->interval);
            zclock_sleep (self->interval);

            if (self->interval < INTERVAL_MAX)
                self->interval *= 2;
            zsock_destroy(&self->frontend);
            self->frontend = s_module_socket (self->name);
            self->liveness = HEARTBEAT_LIVENESS;
        }
        //  Send heartbeat to queue if it's time
        if (zclock_time () > self->heartbeat_at) {
            self->heartbeat_at = zclock_time () + HEARTBEAT_INTERVAL;
            printf ("[Module %s] frontend heartbeat\n", self->name);
            zframe_t *frame = zframe_new (PPP_HEARTBEAT, 1);
            zframe_send (&frame, self->frontend, 0);
        }
  
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
