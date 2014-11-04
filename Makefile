all: client line_controller module device

% : %.c
	gcc $< -lczmq -lzmq -o $@
