//  Pick-n-Pack Device, based on ZeroMQ's Paranoid Pirate worker

#include "czmq.h"
#define HEARTBEAT_LIVENESS  3       //  3-5 is reasonable
#define HEARTBEAT_INTERVAL  1000    //  msecs
#define INTERVAL_INIT       1000    //  Initial reconnect
#define INTERVAL_MAX       32000    //  After exponential backoff

//  Pick-n-Pack Protocol constants for signalling
#define PPP_READY       "\001"      //  Signals device is ready
#define PPP_HEARTBEAT   "\002"      //  Signals device heartbeat

#define STACK_MAX 5 // maximum size of transition stack, e.g. running->configuring->initialising->finalising->pausing when configuring cannot proceed without reinit
#define PAYLOAD_MAX 10 // maximum number of payload items in a single transition. E.g. change 10 configuration parameters.

//  Helper function that returns a new configured socket
//  connected to the Paranoid Pirate queue

typedef enum {
    PIPE,
	NAME
}creating_state_parameters;


static zsock_t *
s_device_socket (char *name, char *link) {
    zsock_t *device = zsock_new_dealer(link); // TODO: this should be configured

    //  Tell Line Controller we're ready for work
    printf ("[%s] socket ready", name);
    zframe_t *frame = zframe_new (PPP_READY, 1);
    zframe_send (&frame, device, 0);

    return device;
}

typedef struct {
    char *name;
    zsock_t *frontend; // socket to frontend process, e.g. module
    zsock_t *frontend_heartbeat; // socket for heartbeat to frontend process, e.g. module
    zsock_t *backend; // socket to potential backend processes, e.g. subdevices
    zsock_t *backend_heartbeat; // socket for heartbeat to backend process, e.g. subdevices
    zsock_t *pipe; // socket to main loop
    size_t liveness; // liveness defines how many heartbeat failures are tolerable
    size_t interval; // interval defines at what interval heartbeats are sent
    uint64_t heartbeat_at; // heartbeat_at defines when to send next heartbeat
} resource_t;

typedef struct {
	char* name;
	void* value;
} payload_item;

typedef struct{
	payload_item *items[PAYLOAD_MAX];
	unsigned int size;//number of payload items
}payload;

typedef int (*state_fnc)(resource_t* self, payload *payload);

typedef struct{
	state_fnc state;
	payload *payload;
} transition;

transition* new_transition(void) {
  return calloc(1,sizeof(transition));
}

payload* new_payload(void) {
  return calloc(1,sizeof(payload));
}

payload_item* new_payload_item(void) {
  return calloc(1,sizeof(payload_item));
}

// assumption:
// <names> and <values> are both of length <size> and smaller than <PAYLOAD_MAX>
static void payload_init(payload* payload, char* names[], void* values[], int size) {
	int i;
	for(i=0;i<size; i++) {
		payload_item* p_i = new_payload_item();
		p_i->name = names[i];
		p_i->value = values[i];
		payload->items[i] = p_i;
	}
	payload->size = size;
}

typedef struct {
    transition *transitions[STACK_MAX];
    unsigned int size;
} transition_stack;

static void transition_stack_init(transition_stack *S){
    S->size = 0;
}

static transition *transition_stack_top(transition_stack *S){
    if (S->size == 0) {
        fprintf(stderr, "Error: stack empty\n");
        return NULL;
    }
    return (S->transitions[S->size-1]);
}

static void transition_stack_push(transition_stack *S, transition *d){
    if (S->size < STACK_MAX){
        S->transitions[S->size++] = d;
    }else{
        fprintf(stderr, "Error: stack full\n");
    }
}

static void transition_stack_pop(transition_stack *S){
    if (S->size == 0){
        fprintf(stderr, "Error: stack empty\n");
    }else{
        S->size--;
    }
}

int creating_fnc(resource_t* self, payload *payload);
int initializing_fnc(resource_t* self, payload *payload);
int configuring_fnc(resource_t* self, payload *payload);
int running_fnc(resource_t* self, payload *payload);
int pausing_fnc(resource_t* self, payload *payload);
int finalizing_fnc(resource_t* self, payload *payload);
int deleting_fnc(resource_t* self, payload *payload);


resource_t* creating(resource_t *self, zsock_t *pipe, char *name) {
    printf("[%s] creating...", name);
    self->name = name;
    self->frontend = s_device_socket(name,"tcp://localhost:9003");
    self->backend = NULL;
    self->pipe = pipe;
    printf("...done.\n");
    return self;
}

int initializing(resource_t* self) {
    printf("[%s] starting...", self->name);
    // send signal on pipe socket to acknowledge initialisation
    zsock_signal (self->pipe, 0);
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
}


int running(resource_t* self) {
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
			if (memcmp (zframe_data (frame), PPP_HEARTBEAT, 1) == 0) {
				printf("[%s] RX HB FRONTEND\n", self->name);
				self->liveness = HEARTBEAT_LIVENESS;
			} else {
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
		printf ("[%s] heartbeat failure, can't reach frontend\n", self->name);
		printf ("[%s] reconnecting in %zd msec...\n", self->name, self->interval);
		zclock_sleep (self->interval);

		if (self->interval < INTERVAL_MAX)
			self->interval *= 2;
		zsock_destroy(&self->frontend);
		self->frontend = zsock_new_dealer("tcp://localhost:9003"); // TODO: this should be configured.
		self->liveness = HEARTBEAT_LIVENESS;
	}
	//  Send heartbeat to queue if it's time
	if (zclock_time () > self->heartbeat_at) {
		self->heartbeat_at = zclock_time () + HEARTBEAT_INTERVAL;
		printf ("[%s] TX HB FRONTEND\n", self->name);
		zframe_t *frame = zframe_new (PPP_HEARTBEAT, 1);
		zframe_send (&frame, self->frontend, 0);
	}

	return 0;
}

int pausing(resource_t *self) {
    printf("[%s] pausing...", self->name);
    printf("done.\n");
    return 0;
}
int finalizing(resource_t *self) {
    printf("[%s] finalizing...", self->name);
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

int creating_fnc(resource_t* self,payload *payload){
	self = creating(self, (*payload->items[0]).value, (*payload->items[1]).value);
    assert(self);
    return 0;
}

int initializing_fnc(resource_t* self, payload *payload) {
	return initializing(self);
}

int configuring_fnc(resource_t* self, payload *payload) {
	return configuring(self);
}

int running_fnc(resource_t* self, payload *payload) {
	while(!zsys_interrupted){

	  if(running(self) < 0){
		  return -1;
	  }

	  //TODO::check messages, if message is for running state process it else return -1
	  //sleep(1);
    }
	return -1;
}

int pausing_fnc(resource_t* self, payload *payload) {

	while(!zsys_interrupted){

	  if(pausing(self) < 0){
		  return -1;
	  }
	  //TODO::check messages, if message is for pausing state process it else return -1
	  //sleep(1);
	}
	return -1;
}

int finalizing_fnc(resource_t* self,payload *payload) {
	return finalizing(self);
}

int deleting_fnc(resource_t* self, payload *payload) {
	return deleting(self);
}

static void device_actor(zsock_t *pipe, void *args){
    char* name = (char*) args;
    printf("[%s] actor started.\n", name);

    /* Go to running state by default */
    resource_t *self = (resource_t *) zmalloc (sizeof (resource_t));

    transition_stack s;
    transition_stack_init(&s);

    transition *running = new_transition();
    running->state = running_fnc;
    running->payload = new_payload();
    transition_stack_push(&s,running);

    transition *configuring = new_transition();
    configuring->state=configuring_fnc;
    configuring->payload = new_payload();
    transition_stack_push(&s,configuring);

    transition *initializing = new_transition();
    initializing->state=initializing_fnc;
    initializing->payload = new_payload();
    transition_stack_push(&s,initializing);

    transition *creating = new_transition();
    creating->state = creating_fnc;
    creating->payload = malloc(sizeof(payload));
    char* names[] = {"pipe","name"};
    void* values[] = {pipe,name};

    payload_init(creating->payload,names,values,2);
    transition_stack_push(&s,creating);

    transition *transition = transition_stack_top(&s);
    while(transition != NULL){
    	transition_stack_pop(&s);
    	int result = transition->state(self,transition->payload);
    	if(result < 0){
    		//Invoke stopping command
    	}
    	transition = transition_stack_top(&s);
    }
    printf("[%s] actor stopped.\n", name);
}

    
/* main */
int main(int argc, char** args)
{
    char* name = NULL;
    if(argc > 1)
        name = args[1];
    else 
        name = "PnP Device";
    assert(name);

    // incoming data is handled by the actor thread
    zactor_t *actor = zactor_new (device_actor, (void*)name);
    assert(actor);
    while(!zsys_interrupted) { sleep(1);};
    printf("[%s] main loop interrupted!\n", name);
    zactor_destroy(&actor);

    return 0;
}
