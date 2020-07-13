#define _GNU_SOURCE

#include "raft.h"

#include <assert.h>
#include <signal.h>
#include <stdbool.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <unistd.h>

#include <sys/prctl.h>
#include <sys/random.h>
#include <sys/socket.h>
#include <sys/types.h>
#include <sys/un.h>
#include <sys/wait.h>

#include <arpa/inet.h>

#define ARR_LENGTH 1000

#define REQUEST_INTERVAL_NS 100000000

static int arr[ARR_LENGTH];

/* Client's view of what's in a Raft log entry.
 * In this case, a simple key-value pair.
 */
struct command {
	size_t index;
	int val;
};

/* Form of a read request: an index to fetch.
 */
struct read_request {
	size_t index;
};

/* Form of read result: key-value pair corresponding
 * to the index looked up.
 */
struct read_result {
	size_t index;
	int val;
};

/* In this case, "applying" an entry means setting the
 * given index in arr to the given value.
 */
void apply_command(const void *data)
{
	struct command cmd;

	memcpy(&cmd, data, sizeof(cmd));
	assert(cmd.index < ARR_LENGTH);
	arr[cmd.index] = cmd.val;

	printf("W: %zu <- %d\n", cmd.index, cmd.val);
}

/* A read-only requests specifies an index, and to
 * resolve it we look up the corresponding entry in
 * arr and tell the server to return it.
 */
void resolve_read(const void *data, void *result)
{
	struct read_request req;
	struct read_result res;

	memcpy(&req, data, sizeof(req));
	assert(req.index < ARR_LENGTH);
	res.index = req.index;
	res.val = arr[res.index];
	memcpy(result, &res, sizeof(res));
}

/* Send a command to the server and wait for a message
 * in response.
 */
static raft_server_response_type submit_command(int server_fd, const struct command *cmd)
{
	uint8_t buf[RAFT_ENTRY_DATA_SIZE];
	uint16_t type = htons(RAFT_WRITE);
	uint16_t status;

	assert(sizeof(*cmd) <= RAFT_ENTRY_DATA_SIZE);
	assert(cmd->index < ARR_LENGTH);

	memset(buf, 0, sizeof(buf));
	memcpy(buf, cmd, sizeof(*cmd));

	if (write(server_fd, &type, sizeof(type)) < 0) {
		perror("submit_command: write");
		_exit(EXIT_FAILURE);
	}

	if (write(server_fd, buf, RAFT_ENTRY_DATA_SIZE) < 0) {
		perror("submit_command: write");
		_exit(EXIT_FAILURE);
	}

	if (read(server_fd, &status, sizeof(status)) < 0) {
		perror("submit_command: read");
		_exit(EXIT_FAILURE);
	}

	return ntohs(status);
}

static raft_server_response_type request_read(
	int server_fd, const struct read_request *req, struct read_result *res)
{
	uint8_t buf_out[RAFT_READ_DATA_SIZE];
	uint8_t buf_in[RAFT_READ_RESULT_SIZE];
	uint16_t type = htons(RAFT_READ);
	uint16_t status;
	uint8_t *p;
	size_t left;
	ssize_t got;

	assert(sizeof(*req) <= RAFT_READ_DATA_SIZE);
	assert(sizeof(*res) <= RAFT_READ_RESULT_SIZE);

	memset(buf_out, 0, sizeof(buf_out));
	memcpy(buf_out, req, sizeof(*req));

	if (write(server_fd, &type, sizeof(type)) < 0) {
		perror("request_read: write");
		_exit(EXIT_FAILURE);
	}

	if (write(server_fd, buf_out, RAFT_READ_DATA_SIZE) < 0) {
		perror("request_read: write");
		_exit(EXIT_FAILURE);
	}

	if (read(server_fd, &status, sizeof(status)) < 0) {
		perror("request_read: read");
		_exit(EXIT_FAILURE);
	}
	status = ntohs(status);

	if (status == RAFT_READ_RESULT) {
		p = buf_in;
		left = RAFT_READ_RESULT_SIZE;
		while (left > 0) {
			if ((got = read(server_fd, p, left)) < 0) {
				perror("raft_receive_command: read");
				_exit(EXIT_FAILURE);
			}
			left -= (size_t)got;
			p += (size_t)got;
		}
	}

	if (res) {
		memcpy(res, buf_in, sizeof(*res));
	}

	return status;
}

/* Wait for a message indicating that the attached server
 * is the leader.
 */
static void await_leader(int server_fd)
{
	raft_server_response_type status;

	do {
		if (read(server_fd, &status, sizeof(status)) < 0) {
			perror("await_leader: read");
			_exit(EXIT_FAILURE);
		}
		status = ntohs(status);
	} while (status == RAFT_NOT_LEADER);

	assert(status == RAFT_BECAME_LEADER);
}

static void drive_server(int server_fd)
{
	await_leader(server_fd);

	while (true) {
		struct timespec spec = {.tv_sec = 0, .tv_nsec = REQUEST_INTERVAL_NS};
		struct command cmd;
		struct read_request req;
		struct read_result res;
		raft_server_response_type status;
		bool deposed = false;

		cmd.index = 0;
		cmd.val = (int)random();

		req.index = (size_t)random() % ARR_LENGTH;

		while (true) {
			if ((status = submit_command(server_fd, &cmd)) == RAFT_COMMAND_APPLIED) {
				cmd.index = (cmd.index + 1) % ARR_LENGTH;
				nanosleep(&spec, NULL);
			} else if (status == RAFT_NOT_LEADER) {
				deposed = true;
				break;
			} else {
				// The server lost and then regained leadership before we had
				// the chance to submit a request.
				assert(status == RAFT_BECAME_LEADER);
				break;
			}

			if ((status = request_read(server_fd, &req, &res)) == RAFT_READ_RESULT) {
				req.index = (size_t)random() % ARR_LENGTH;
				nanosleep(&spec, NULL);
			} else if (status == RAFT_NOT_LEADER) {
				deposed = true;
				break;
			} else {
				assert(status == RAFT_BECAME_LEADER);
				break;
			}
		}

		if (deposed) {
			await_leader(server_fd);
		}
	}
}

static void server_exit_handler(int sig)
{
	(void)sig;

	waitpid(-1, NULL, WNOHANG);

	_exit(EXIT_SUCCESS);
}

static void usage(void)
{
	fprintf(stderr,
		"Usage: driver [-R] -i SERVER_ID -c CONFIG_PATH [-p PERSIST_PATH] [-s RANDOM_SEED]\n"
		"Servers are numbered according to the order in the config file, starting at 1.\n"
		"In recovery mode (-R specified), an existing persist file must be specified with -p.\n"
		"In normal mode (-R not specified), a persist path may be specified;\n"
		"\tif so, it must not refer to an existing file.\n"
		"The server process prints a (very noisy) log of sent and received messages to\n"
		"\tstandard error, and a (much quieter) history of applied writes to\n"
		"\tstandard output.\n");
}

int main(int argc, char *argv[])
{
	enum raft_mode mode = RAFT_NORMAL_MODE;
	int opt;
	char *server_id_str = NULL, *config_path = NULL, *persist_path = NULL,
		 *seed_str = NULL;
	unsigned server_id;
	unsigned seed;
	int fds[2];
	struct sigaction act;
	pid_t pid;

	while ((opt = getopt(argc, argv, "Ri:c:p:s:")) != -1) {
		switch (opt) {
		case 'R':
			mode = RAFT_RECOVER_MODE;
			break;
		case 'i':
			server_id_str = optarg;
		case 'c':
			config_path = optarg;
			break;
		case 'p':
			persist_path = optarg;
			break;
		case 's':
			seed_str = optarg;
			break;
		default:
			usage();
			_exit(EXIT_FAILURE);
		}
	}

	if (!server_id_str || !config_path) {
		usage();
		_exit(EXIT_FAILURE);
	}
	server_id = (unsigned)strtol(server_id_str, NULL, 10);

	if (mode == RAFT_RECOVER_MODE && !persist_path) {
		usage();
		_exit(EXIT_FAILURE);
	}

	if (seed_str) {
		seed = (unsigned)strtol(seed_str, NULL, 10);
	} else {
		getrandom(&seed, sizeof(seed), 0);
	}

	if (socketpair(AF_LOCAL, SOCK_STREAM, 0, fds) < 0) {
		perror("main: socketpair");
		_exit(EXIT_FAILURE);
	}

	// Clean up the driver process if the server exits.
	sigemptyset(&(act.sa_mask));
	act.sa_handler = server_exit_handler;
	if (sigaction(SIGCHLD, &act, NULL) < 0) {
		perror("main: sigaction");
		_exit(EXIT_FAILURE);
	}

	pid = fork();
	if (pid < 0) {
		perror("main: fork");
		_exit(EXIT_FAILURE);
	} else if (pid > 0) {
		srandom(seed);

		drive_server(fds[0]);
	} else {
		// Clean up the server process (rudely) if its parent exits.
		if (prctl(PR_SET_PDEATHSIG, SIGTERM, 0, 0, 0) < 0) {
			perror("main: prctl");
			_exit(EXIT_FAILURE);
		}

		srandom(seed);

		raft_main(mode, server_id, config_path, persist_path, fds[1]);
	}
}
