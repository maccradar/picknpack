all: client plant line module device

% : %.c
	gcc $< -lczmq -lzmq -o $@
