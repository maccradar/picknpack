#ifndef PNP_DEFS
#define PNP_DEFS "Pick-n-Pack Definitions"

// Ready message contains 2 frames: ID, READY
// Heartbeat message contains 3 frames: ID, STATE, SIGNAL/COMMAND
// Data message contains 4 frames: ID, STATE, SIGNAL/COMMAND, PAYLOAD

//  Pick-n-Pack Protocol constants for signalling
#define PNP_READY "\001"    //  Signals device is ready
#define PNP_HEARTBEAT "\002"      //  Signals device heartbeat


// IDs
#define PNP_LINE_ID "\010"
#define PNP_THERMOFORMER_ID "\011"
#define PNP_ROBOT_CELL_ID "\012"
#define PNP_QAS_ID "\013"
#define PNP_CEILING_ID "\014"
#define PNP_PRINTING_ID "\015"

// Names
#define PNP_LINE "Line"
#define PNP_THERMOFORMER "Thermoformer"
#define PNP_ROBOT_CELL "Robot Cell"
#define PNP_QAS "QAS"
#define PNP_CEILING "Ceiling"
#define PNP_PRINTING "Printing"

// Status
#define PNP_CREATING "\100"
#define PNP_INITIALISING "\101"
#define PNP_CONFIGURING "\102"
#define PNP_RUNNING "\103"
#define PNP_PAUSING "\104"
#define PNP_FINALISING "\105"
#define PNP_DELETING "\106"

// Signals/commands
#define PNP_RUN "\110"
#define PNP_PAUSE "\111"
#define PNP_CONFIGURE "\112"
#define PNP_STOP "\113"
#define PNP_REBOOT "\114"

// Error codes
#define PNP_ERR_HEARTBEAT "\121"
#define PNP_ERR_FPGA "\122"
#define PNP_ERR_CAMERA "\123"
#define PNP_ERR_MISSING_PARAMETER "\124"
#define PNP_ERR_HDF5 "\125"
#define PNP_ERR_LOG "\126"
#define PNP_ERR_UNDEFINED "\127"

// Resource definitions
#define HEARTBEAT_LIVENESS  3       //  3-5 is reasonable
#define HEARTBEAT_INTERVAL  1000    //  msecs
#define INTERVAL_INIT       1000    //  Initial reconnect
#define INTERVAL_MAX       32000    //  After exponential backoff



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
    char *uuid;					//  Pick-n-Pack universal unique identifier
    char *id_string;            //  Printable identity
    int64_t expiry;             //  Expires at this time
} backend_resource_t;

static char* uuid_to_name(char* uuid) {
	if(strncmp(uuid,PNP_LINE_ID,3) == 0)
		return PNP_LINE;
	if(strncmp(uuid,PNP_THERMOFORMER_ID,3) == 0)
		return PNP_THERMOFORMER;
	if(strncmp(uuid,PNP_QAS_ID,3) == 0)
		return PNP_QAS;
	if(strncmp(uuid,PNP_ROBOT_CELL_ID,3) == 0)
		return PNP_ROBOT_CELL;
	if(strncmp(uuid,PNP_CEILING_ID,3) == 0)
		return PNP_CEILING;
	if(strncmp(uuid,PNP_PRINTING_ID,3) == 0)
		return PNP_PRINTING;
	return "unknown";
}

//  Construct new resource, i.e. new local object representing a resource at the backend
static backend_resource_t *
s_backend_resource_new (zframe_t *identity, char* uuid)
{
    backend_resource_t *self = (backend_resource_t *) zmalloc (sizeof (backend_resource_t));
    self->identity = identity;
    self->uuid = uuid;
    self->id_string = uuid_to_name(uuid);//zframe_strhex (identity);
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

// Forward declarations
resource_t* creating(resource_t *self, zsock_t *pipe, char *name);
int initializing(resource_t* self);
int configuring (resource_t* self);
int running(resource_t *self);
int pausing(resource_t *self);
int finalizing(resource_t *self);
int deleting(resource_t *self);


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

#endif
