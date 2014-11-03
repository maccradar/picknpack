//  Pick-n-Pack client, based on ZeroMQ's Paranoid Pirate client
//  Use zmq_poll to do a safe request-reply

#include "czmq.h"
#define REQUEST_TIMEOUT     2500    //  msecs, (> 1000!)
#define REQUEST_RETRIES     3       //  Before we abandon
#define SERVER_ENDPOINT     "tcp://localhost:5555"

int main (void)
{
    printf ("I: connecting to line controller...\n");
    zsock_t *client = zsock_new_dealer (SERVER_ENDPOINT);
    assert (client);

    int sequence = 0;
    int retries_left = REQUEST_RETRIES;
    while (retries_left && !zsys_interrupted) {
        //  We send a request, then we work to get a reply
        char request [10];
        sprintf (request, "%d", ++sequence);
	printf("Sending request (%s)", request);
        zstr_send (client, request);

        int expect_reply = 1;
        while (expect_reply) {
            //  Poll socket for a reply, with timeout
            zmq_pollitem_t items [] = { { zsock_resolve(client), 0, ZMQ_POLLIN, 0 } };
            int rc = zmq_poll (items, 1, REQUEST_TIMEOUT * ZMQ_POLL_MSEC);
            if (rc == -1)
                break;          //  Interrupted

            //  .split process server reply
            //  Here we process a server reply and exit our loop if the
            //  reply is valid. If we didn't a reply we close the client
            //  socket and resend the request. We try a number of times
            //  before finally abandoning:
            
            if (items [0].revents & ZMQ_POLLIN) {
                //  We got a reply from the server, must match sequence
                char *reply = zstr_recv (client);
                if (!reply)
                    break;      //  Interrupted
                if (atoi (reply) == sequence) {
                    printf ("I: line controller replied (%s)\n", reply);
                    retries_left = REQUEST_RETRIES;
                    expect_reply = 0;
                }
                else
                    printf ("E: malformed reply from line controller: %s\n",
                        reply);

                free (reply);
            }
            else
            if (--retries_left == 0) {
                printf ("E: line controller seems to be offline, abandoning\n");
                break;
            }
            else {
                printf ("W: no response from line controller, retrying...\n");
                //  Old socket is confused; close it and open a new one
                zsock_destroy (&client);
                printf ("I: reconnecting to line controller...\n");
                client = zsock_new (ZMQ_DEALER);
                zsock_connect (client, SERVER_ENDPOINT);
                //  Send request again, on new socket
                zstr_send (client, request);
            }
        }
    }
    return 0;
}
