//  Pick-n-Pack Line, based on ZeroMQ's Paranoid Pirate worker
#include "czmq.h"
#include "defs.h"

#define HEARTBEAT_LIVENESS  3       //  3-5 is reasonable
#define HEARTBEAT_INTERVAL  1000    //  msecs
#define INTERVAL_INIT       1000    //  Initial reconnect
#define INTERVAL_MAX       32000    //  After exponential backoff

//  Pick-n-Pack Protocol constants for signalling
//#define PPP_READY       "\001"      //  Signals device is ready
#define PPP_HEARTBEAT   "\002"      //  Signals device heartbeat

#define STACK_MAX 5 // maximum size of transition stack, e.g. running->configuring->initialising->finalising->pausing when configuring cannot proceed without reinit
#define PAYLOAD_MAX 10 // maximum number of payload items in a single transition. E.g. change 10 configuration parameters.

typedef enum states{
	STATE_CREATING,
	STATE_INITIALIZING,
	STATE_CONFIGURING,
	STATE_RUNNING,
	STATE_PAUSING,
	STATE_FINALIZING,
	STATE_DELETING,
	NUM_STATES,
	NO_STATE
}state;

typedef enum signals{
	SIGNAL_RUN,
	SIGNAL_PAUSE,
	SIGNAL_STOP,
	SIGNAL_CONFIGURE,
	SIGNAL_REBOOT,
  	NUM_SIGNALS
}signl;

state transitions[NUM_STATES][NUM_SIGNALS] = {
						 		/*SIGNAL_RUN		SIGNAL_PAUSE        SIGNAL_STOP      	SIGNAL_CONFIGURE 	SIGNAL_REBOOT */
	/*STATE_CREATING*/     	{  	STATE_INITIALIZING, STATE_INITIALIZING, STATE_INITIALIZING, STATE_INITIALIZING, STATE_INITIALIZING},
	/*STATE_INITIALIZING*/ 	{  	STATE_CONFIGURING,  STATE_CONFIGURING,  STATE_CONFIGURING,	STATE_CONFIGURING,	NO_STATE},
	/*STATE_CONFIGURING*/  	{  	STATE_RUNNING,      STATE_PAUSING,    	STATE_PAUSING,    	NO_STATE,  			STATE_PAUSING},
	/*STATE_RUNNING*/      	{  	NO_STATE,			STATE_PAUSING,    	STATE_PAUSING,      STATE_CONFIGURING,  STATE_PAUSING},
	/*STATE_PAUSING*/      	{  	STATE_RUNNING,      NO_STATE,    		STATE_FINALIZING,   STATE_CONFIGURING,  STATE_FINALIZING},
	/*STATE_FINALIZING*/   	{  	STATE_INITIALIZING, STATE_INITIALIZING,	STATE_DELETING,     STATE_INITIALIZING, STATE_INITIALIZING},
	/*STATE_DELETING*/     	{  	NO_STATE,         	NO_STATE,         	NO_STATE,         	NO_STATE,         	NO_STATE}
};



typedef struct {
    zframe_t *identity;         //  Identity of resource
    char *id_string;            //  Printable identity
    int64_t expiry;             //  Expires at this time
} backend_resource_t;

//  Construct new resource, i.e. new local object representing a resource at the backend
static backend_resource_t *
s_backend_resource_new (zframe_t *identity)
{
    backend_resource_t *self = (backend_resource_t *) zmalloc (sizeof (backend_resource_t));
    self->identity = identity;
    self->id_string = zframe_strhex (identity);
    self->expiry = zclock_time ()
                 + HEARTBEAT_INTERVAL * HEARTBEAT_LIVENESS;
    return self;
}

//  Destroy specified backend_resource object, including identity frame.
static void
s_backend_resource_destroy (backend_resource_t **self_p)
{
    assert (self_p);
    if (*self_p) {
        backend_resource_t *self = *self_p;
        zframe_destroy (&self->identity);
        free (self->id_string);
        free (self);
        *self_p = NULL;
    }
}

//  The ready method puts a backend_resource to the end of the ready list:

static void
s_backend_resource_ready (backend_resource_t *self, zlist_t *backend_resources)
{
    backend_resource_t *backend_resource = (backend_resource_t *) zlist_first (backend_resources);
    while (backend_resource) {
        if (streq (self->id_string, backend_resource->id_string)) {
            zlist_remove (backend_resources, backend_resource);
            s_backend_resource_destroy (&backend_resource);
            break;
        }
        backend_resource = (backend_resource_t *) zlist_next (backend_resources);
    }
    zlist_append (backend_resources, self);
}

//  The next method returns the next available backend_resource identity:
//  TODO: Not all backend_resources have the same capabilities so we should not just pick any backend_resource...

static zframe_t *
s_backend_resources_next (zlist_t *backend_resources)
{
    backend_resource_t *backend_resource = zlist_pop (backend_resources);
    assert (backend_resource);
    zframe_t *frame = backend_resource->identity;
    backend_resource->identity = NULL;
    s_backend_resource_destroy (&backend_resource);
    return frame;
}

//  The purge method looks for and kills expired backend_resources. We hold backend_resources
//  from oldest to most recent, so we stop at the first alive backend_resource:
//  TODO: if backend_resources expire, it should be checked if this effects the working of the line!

static void
s_backend_resources_purge (zlist_t *backend_resources)
{
    backend_resource_t *backend_resource = (backend_resource_t *) zlist_first (backend_resources);
    while (backend_resource) {
        if (zclock_time () < backend_resource->expiry)
            break;              //  backend_resource is alive, we're done here
	printf("I: Removing expired backend_resource %s\n", backend_resource->id_string);
        zlist_remove (backend_resources, backend_resource);
        s_backend_resource_destroy (&backend_resource);
        backend_resource = (backend_resource_t *) zlist_first (backend_resources);
    }
}

typedef struct {
    char *name;
    zsock_t *frontend; // socket to frontend process, e.g. backend_resource
    zsock_t *backend; // socket to potential backend processes, e.g. subdevices
    zsock_t *pipe; // socket to main loop
    size_t liveness; // liveness defines how many heartbeat failures are tolerable
    size_t interval; // interval defines at what interval heartbeats are sent
    uint64_t heartbeat_at; // heartbeat_at defines when to send next heartbeat
    zlist_t *backend_resources;
    zlist_t *required_resources;
} resource_t;

typedef struct {
	char* name;
	void* value;
} payload_item;

typedef struct{
	payload_item *items[PAYLOAD_MAX];
	unsigned int size;//number of payload items
}payload;


typedef struct{
	state state;
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
        //fprintf(stderr, "Error: stack empty\n");
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

typedef int (*state_fnc)(resource_t* self, payload *payload);



int creating_fnc(resource_t* self, payload *payload);
int initializing_fnc(resource_t* self, payload *payload);
int configuring_fnc(resource_t* self, payload *payload);
int running_fnc(resource_t* self, payload *payload);
int pausing_fnc(resource_t* self, payload *payload);
int finalizing_fnc(resource_t* self, payload *payload);
int deleting_fnc(resource_t* self, payload *payload);

state_fnc state_functions[NUM_STATES] = {
	/*STATE_CREATING*/     	 	creating_fnc,
	/*STATE_INITIALIZING*/ 	 	initializing_fnc,
	/*STATE_CONFIGURING*/  		configuring_fnc,
	/*STATE_RUNNING*/      	 	running_fnc,
	/*STATE_PAUSING*/      	 	pausing_fnc,
	/*STATE_FINALIZING*/   	 	finalizing_fnc,
	/*STATE_DELETING*/     	 	deleting_fnc
};

resource_t* creating(resource_t *self, zsock_t *pipe, char *name) {
    printf("[%s] creating...", name);
    self->name = name;
    self->frontend = zsock_new_dealer("tcp://localhost:9001"); // TODO: this should be configured
    self->backend =  zsock_new_router ("tcp://*:9002"); // TODO: this should be configured
    self->pipe = pipe;
    self->backend_resources = zlist_new ();
    self->required_resources = zlist_new();
    printf("...done.\n");
    return self;
}

int initializing(resource_t* self) {
    printf("[%s] initializing...", self->name);
    // send signal on pipe socket to acknowledge initialization
    zsock_signal (self->pipe, 0);
    zlist_push(self->required_resources, PNP_QAS);
    zlist_push(self->required_resources, PNP_PRINTING);
    zframe_t *frame = zframe_new (READY, 1);
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
}

int running(resource_t *self) {
    //  Send out heartbeats at regular intervals
    uint64_t heartbeat_at = zclock_time () + HEARTBEAT_INTERVAL;

	zmq_pollitem_t items [] = {
		{ zsock_resolve(self->backend),  0, ZMQ_POLLIN, 0 },
		{ zsock_resolve(self->frontend), 0, ZMQ_POLLIN, 0 }
	};
	//  Poll frontend only if we have available backend_resources
	int rc = zmq_poll (items, zlist_size (self->backend_resources)? 2: 1,
		HEARTBEAT_INTERVAL * ZMQ_POLL_MSEC);
	if (rc == -1) {
		printf("E: Line Controller failed to poll sockets\n");
		return -1;              //  Interrupted
	}
	//  Handle backend_resource activity on backend
	if (items [0].revents & ZMQ_POLLIN) {
		//  Use backend_resource identity for identify resource location
		zmsg_t *msg = zmsg_recv (self->backend);
		if (!msg)
			return -1;          //  Interrupted

		//  Any sign of life from backend_resource means it's ready
		zframe_t *identity = zmsg_unwrap (msg);
		backend_resource_t *backend_resource = s_backend_resource_new (identity);
		s_backend_resource_ready (backend_resource, self->backend_resources);

		//  Validate control message, or return reply to client
		if (zmsg_size (msg) == 2) {
			// ID
			zframe_t *frame = zmsg_first (msg);
			printf("[%s] RX MSG FROM %s\n", self->name, zframe_strhex(frame));
			frame = zmsg_next(msg);
			if (memcmp (zframe_data (frame), READY, 0)) {
				printf("[%s] RX HB BACKEND %s\n", self->name, backend_resource->id_string);
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
			if (memcmp (zframe_data (frame), READY, 1)
			&&  memcmp (zframe_data (frame), PPP_HEARTBEAT, 1)) {
				printf ("E: invalid message from backend_resource\n");
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
		//zframe_t *identity = s_backend_resources_next (self->backend_resources);
		//zmsg_prepend (msg, &identity);
		//zmsg_send (&msg, backend);
	}
	//  .split handle heartbeating
	//  We handle heartbeating after any socket activity. First, we send
	//  heartbeats to any idle backend_resources if it's time. Then, we purge any
	//  dead backend_resources:
	if (zclock_time () >= heartbeat_at) {
		backend_resource_t *backend_resource = (backend_resource_t *) zlist_first (self->backend_resources);
		while (backend_resource) {
			zframe_send (&backend_resource->identity, self->backend,	ZFRAME_REUSE + ZFRAME_MORE);
			zframe_t *frame = zframe_new (PPP_HEARTBEAT, 1);
			zframe_send (&frame, self->backend, 0);
			printf("[%s] TX HB BACKEND %s\n", self->name, backend_resource->id_string);
			backend_resource = (backend_resource_t *) zlist_next (self->backend_resources);
		}
		// Send heartbeat to frontend
		zframe_t *frame = zframe_new (PPP_HEARTBEAT, 1);
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

static void generate_stack(transition_stack *s, state state, signl signal){
	transition_stack t;
	transition_stack_init(&t);

	transition *temp;

	//get first transition to the next state
	state = transitions[state][signal];

	//use transition table to generate path from initial state to state where signal leads to 'NO_STATE'.
	while(state != NO_STATE){
		printf("%d \n", state);
		temp = new_transition();
		temp->state = state;
		temp->payload = new_payload();
		transition_stack_push(&t,temp);
		state = transitions[state][signal];
	}

	//reverse to path to make it suitable for the stack.
	temp = transition_stack_top(&t);
	while(temp != NULL){
		transition_stack_pop(&t);
		transition_stack_push(s,temp);
		temp = transition_stack_top(&t);
	}
}

static void resource_actor(zsock_t *pipe, void *args){
    char* name = (char*) args;
    printf("[%s] actor started.\n", name);

    resource_t *self = (resource_t *) zmalloc (sizeof (resource_t));

    transition_stack s;
    transition_stack_init(&s);

    //setup initial conditions
    state initial_state = STATE_CREATING;
    signl initial_signal = SIGNAL_RUN;

    //move to initial state
    //NOTE: use of initial_state as variables questionable
    transition *t = new_transition();
    t->state = initial_state;
    t->payload = malloc(sizeof(payload));
    char* names[] = {"pipe","name"};
    void* values[] = {pipe,name};
    payload_init(t->payload,names,values,2);

    //generate path for initial state and signal
    generate_stack(&s,initial_state,initial_signal);
    transition_stack_push(&s,t);//add transition to initial state last, since we have a stack

    transition *transition = transition_stack_top(&s);
    while(transition != NULL){
    	transition_stack_pop(&s);//remove transition from stack

    	//act
    	int result = state_functions[transition->state](self,transition->payload);
    	if(result < 0){
    		//TODO::invoke stopping signal
    		pausing(self);
    		finalizing(self);
    		deleting(self);
    		break;
    	}
    	free(transition);

    	//TODO:: check communication lines for signals

    	//get next state
    	transition = transition_stack_top(&s);
    }



    printf("[%s] actor stopped.\n", name);
}


/* main */
int main(int argc, char** args)
{
    char* name = NULL;
    if(argc > 1){
        name = args[1];
    }else{
        name = "PnP Line";
    }
	assert(name);

    // incoming data is handled by the actor thread
    zactor_t *actor = zactor_new (resource_actor, (void*)name);
    assert(actor);
    while(!zsys_interrupted) { sleep(1);};
    printf("[%s] main loop interrupted!\n", name);
    zactor_destroy(&actor);

    return 0;
}


