#ifndef PNP_DEFS
#define PNP_DEFS "Pick-n-Pack Definitions"

// Ready message contains 2 frames: ID, READY
// Heartbeat message contains 3 frames: ID, STATE, SIGNAL/COMMAND
// Data message contains 4 frames: ID, STATE, SIGNAL/COMMAND, PAYLOAD

// IDs
#define PNP_LINE_ID "\050"
#define PNP_THERMOFORMER_ID "\051"
#define PNP_ROBOT_CELL_ID "\052"
#define PNP_QAS_ID "\053"
#define PNP_CEILING_ID "\054"
#define PNP_PRINTING_ID "\055"

// Names
#define PNP_LINE "Line"
#define PNP_THERMOFORMER "Thermoformer"
#define PNP_ROBOT_CELL "Robot Cell"
#define PNP_QAS "QAS"
#define PNP_CEILING "Ceiling"
#define PNP_PRINTING "Printing"

// Status
#define CREATING "\100"
#define INITIALISING "\101"
#define CONFIGURING "\102"
#define RUNNING "\103"
#define PAUSING "\104"
#define FINALISING "\105"
#define DELETING "\106"

// Signals/commands
#define RUN "\200"
#define PAUSE "\201"
#define CONFIGURE "\202"
#define STOP "\203"
#define REBOOT "\204"

// Error codes
#define ERR_HEARTBEAT "\301"
#define ERR_FPGA "\302"
#define ERR_CAMERA "\303"
#define ERR_MISSING_PARAMETER "\304"
#define ERR_HDF5 "\305"
#define ERR_LOG "\306"
#define ERR_UNDEFINED "\307"

#define READY "\001"

#endif
