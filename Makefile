all: client line_controller module

%.o : %.c
	gcc -c $< -o $@
