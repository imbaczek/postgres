#define _GNU_SOURCE
#include <libpq-fe.h>
#include <errno.h>
#include <fcntl.h>
#include <inttypes.h>
#include <limits.h>
#include <poll.h>
#include <signal.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/prctl.h>
#include <sys/resource.h>
#include <time.h>
#include <unistd.h>

#define HEADER_SIZE 25
#define REQUEST_MAX (1024U * 1024U)
#define RESULT_MAX (16U * 1024U * 1024U + 65536U)
#define MEMORY_LIMIT (256U * 1024U * 1024U)
#define COLUMN_MAX 64
#define COPY_RECEIVE_MAX (128U * 1024U + 25U)
#define COPY_SEND_MAX (65536U - 4U)
#define OPERATION_MS 10000

static uint64_t session_id;
static uint64_t request_id;
static PGconn *connection;
static PGresult *result;
static unsigned char *request_body;
static char *copy_data;
static enum { COMMAND_MODE, COPY_BOTH_MODE, COPY_IN_MODE } phase;

static void
put32(unsigned char *out, uint32_t value)
{
	for (int i = 3; i >= 0; i--)
	{
		out[i] = value & 255;
		value >>= 8;
	}
}

static void
put64(unsigned char *out, uint64_t value)
{
	for (int i = 7; i >= 0; i--)
	{
		out[i] = value & 255;
		value >>= 8;
	}
}

static uint32_t
get32(const unsigned char *in)
{
	uint32_t value = 0;
	for (int i = 0; i < 4; i++)
		value = (value << 8) | in[i];
	return value;
}

static uint64_t
get64(const unsigned char *in)
{
	uint64_t value = 0;
	for (int i = 0; i < 8; i++)
		value = (value << 8) | in[i];
	return value;
}

static int
read_exact(void *buffer, size_t size)
{
	unsigned char *next = buffer;
	size_t done = 0;
	while (done < size)
	{
		ssize_t count = read(STDIN_FILENO, next + done, size - done);
		if (count < 0 && errno == EINTR)
			continue;
		if (count <= 0)
			return done == 0 && count == 0 ? 0 : -1;
		done += count;
	}
	return 1;
}

static void
write_exact(const void *buffer, size_t size)
{
	const unsigned char *next = buffer;
	while (size)
	{
		ssize_t count = write(STDOUT_FILENO, next, size);
		if (count < 0 && errno == EINTR)
			continue;
		if (count <= 0)
			_exit(1);
		next += count;
		size -= count;
	}
}

static void
reply(unsigned char kind, const void *body, uint32_t size)
{
	unsigned char header[HEADER_SIZE];
	memcpy(header, "PGLQ", 4);
	put64(header + 4, session_id);
	put64(header + 12, request_id);
	header[20] = kind;
	put32(header + 21, size);
	write_exact(header, sizeof(header));
	if (size)
		write_exact(body, size);
}

static void
cleanup(void)
{
	if (result)
		PQclear(result);
	if (connection)
		PQfinish(connection);
	PQfreemem(copy_data);
	free(request_body);
}

static void
fail(const char *state, const char *message)
{
	unsigned char body[512];
	size_t size = strlen(message);
	if (size > sizeof(body) - 9)
		size = sizeof(body) - 9;
	memcpy(body, state, 5);
	put32(body + 5, size);
	memcpy(body + 9, message, size);
	reply('!', body, size + 9);
	cleanup();
	exit(1);
}

static int64_t
milliseconds(void)
{
	struct timespec now;
	if (clock_gettime(CLOCK_MONOTONIC, &now) != 0)
		fail("58000", "helper monotonic clock failed");
	return (int64_t) now.tv_sec * 1000 + now.tv_nsec / 1000000;
}

static int
poll_socket(short events, int64_t deadline)
{
	struct pollfd descriptors[2] = {
		{PQsocket(connection), events, 0},
		{STDIN_FILENO, POLLIN, 0}
	};
	if (descriptors[0].fd < 0)
		fail("08006", "libpq connection has no socket");
	int status;
	do
	{
		int64_t remaining = deadline - milliseconds();
		if (remaining <= 0)
			return 0;
		status = poll(descriptors, 2, remaining > INT_MAX ? INT_MAX : (int) remaining);
	}
	while (status < 0 && errno == EINTR);
	if (status < 0)
		fail("08006", "libpq socket wait failed");
	if (descriptors[1].revents)
		fail("08P01", "parent closed or overlapped a helper request");
	return status != 0;
}

static void
wait_socket(short events, int64_t deadline)
{
	if (!poll_socket(events, deadline))
		fail("08006", "libpq operation deadline exceeded");
}

static char *
read_string(const unsigned char **next, const unsigned char *end)
{
	if ((size_t) (end - *next) < 4)
		fail("08P01", "helper request lacks a string length");
	uint32_t size = get32(*next);
	*next += 4;
	if (size > (size_t) (end - *next) || memchr(*next, 0, size))
		fail("08P01", "helper request has an invalid string");
	char *text = malloc((size_t) size + 1);
	if (!text)
		fail("53000", "helper memory exhausted");
	memcpy(text, *next, size);
	text[size] = 0;
	*next += size;
	return text;
}

static void
discard_notice(void *argument, const char *message)
{
	(void) argument;
	(void) message;
}

static void
check_multihost(void)
{
	PQconninfoOption *options = PQconninfo(connection);
	if (!options)
		fail("53000", "libpq option inspection failed");
	int multiple = 0;
	for (PQconninfoOption *option = options; option->keyword; option++)
		if (option->val &&
			(strcmp(option->keyword, "host") == 0 ||
			 strcmp(option->keyword, "hostaddr") == 0 ||
			 strcmp(option->keyword, "port") == 0) && strchr(option->val, ','))
			multiple = 1;
	PQconninfoFree(options);
	if (multiple)
		fail("0A000", "native physical follow requires a single endpoint");
}

static void
open_connection(const unsigned char *body, uint32_t size)
{
	if (connection)
		fail("08P01", "helper connection is already open");
	const unsigned char *next = body;
	char *conninfo = read_string(&next, body + size);
	char *fallback = read_string(&next, body + size);
	if (next != body + size)
		fail("08P01", "helper open request has trailing bytes");
	const char *keys[] = {"dbname", "replication", "client_encoding", "fallback_application_name", NULL};
	const char *values[] = {conninfo, "database", "UTF8", fallback, NULL};
	int64_t deadline = milliseconds() + OPERATION_MS;
	connection = PQconnectStartParams(keys, values, 1);
	free(conninfo);
	free(fallback);
	if (!connection)
		fail("08001", "libpq connection failed");
	PQsetNoticeProcessor(connection, discard_notice, NULL);
	check_multihost();
	if (PQstatus(connection) == CONNECTION_BAD)
		fail("08001", "libpq connection failed");
	for (;;)
	{
		PostgresPollingStatusType status = PQconnectPoll(connection);
		if (status == PGRES_POLLING_OK)
			break;
		if (status == PGRES_POLLING_FAILED)
			fail("08001", "libpq connection failed");
		if (milliseconds() >= deadline)
			fail("08001", "libpq connection deadline exceeded");
		if (status != PGRES_POLLING_ACTIVE)
			wait_socket(status == PGRES_POLLING_READING ? POLLIN : POLLOUT, deadline);
	}
	if (PQsetnonblocking(connection, 1) != 0)
		fail("08006", "libpq nonblocking setup failed");
	reply('O', NULL, 0);
}

static void
flush_output(int64_t deadline)
{
	int status;
	while ((status = PQflush(connection)) == 1)
		wait_socket(POLLOUT, deadline);
	if (status < 0)
		fail("08006", "libpq output failed");
}

static PGresult *
next_result(int64_t deadline)
{
	while (PQisBusy(connection))
	{
		wait_socket(POLLIN, deadline);
		if (!PQconsumeInput(connection))
			fail("08006", "libpq input failed");
	}
	return PQgetResult(connection);
}

static size_t
add_size(size_t total, size_t extra, uint32_t maximum)
{
	if (extra > maximum || total > maximum - extra)
		fail("08P01", "native result exceeds its command bound");
	return total + extra;
}

static unsigned char *
append_string(unsigned char *next, const char *value, uint32_t size)
{
	put32(next, size);
	memcpy(next + 4, value, size);
	return next + 4 + size;
}

static void
emit_result(uint32_t maximum, unsigned char kind)
{
	int fields = PQnfields(result);
	int rows = PQntuples(result);
	const char *tag = PQcmdStatus(result);
	if (fields < 0 || fields > COLUMN_MAX || rows < 0 || rows > 1)
		fail("08P01", "native result has an unsupported shape");
	size_t size = add_size(9, 4 + strlen(tag), maximum);
	for (int column = 0; column < fields; column++)
		size = add_size(size, 28 + strlen(PQfname(result, column)), maximum);
	for (int row = 0; row < rows; row++)
		for (int column = 0; column < fields; column++)
			size = add_size(size, 4 + (PQgetisnull(result, row, column) ? 0 : (size_t) PQgetlength(result, row, column)), maximum);
	unsigned char *body = malloc(size);
	if (!body)
		fail("53000", "helper memory exhausted");
	body[0] = PQtransactionStatus(connection);
	unsigned char *next = append_string(body + 1, tag, strlen(tag));
	put32(next, fields);
	put32(next + 4, rows);
	next += 8;
	for (int column = 0; column < fields; column++)
	{
		const char *name = PQfname(result, column);
		next = append_string(next, name, strlen(name));
		put32(next, PQftable(result, column));
		put32(next + 4, PQftablecol(result, column));
		put32(next + 8, PQftype(result, column));
		put32(next + 12, PQfsize(result, column));
		put32(next + 16, PQfmod(result, column));
		put32(next + 20, PQfformat(result, column));
		next += 24;
	}
	for (int row = 0; row < rows; row++)
		for (int column = 0; column < fields; column++)
			if (PQgetisnull(result, row, column))
			{
				put32(next, UINT32_MAX);
				next += 4;
			}
			else
				next = append_string(next, PQgetvalue(result, row, column), PQgetlength(result, row, column));
	if ((size_t) (next - body) != size)
		fail("XX000", "helper result encoding failed");
	reply(kind, body, size);
	free(body);
}

static void
finish_result(uint32_t maximum, int64_t deadline)
{
	if (!result)
		fail("08P01", "native query lacks a result");
	ExecStatusType status = PQresultStatus(result);
	if (status != PGRES_TUPLES_OK && status != PGRES_COMMAND_OK)
	{
		const char *state = PQresultErrorField(result, PG_DIAG_SQLSTATE);
		fail(state && strlen(state) == 5 ? state : "08P01", "native source command failed");
	}
	PGresult *extra = next_result(deadline);
	if (extra)
	{
		PQclear(extra);
		fail("08P01", "native query returned extra results");
	}
	if (PQtransactionStatus(connection) != PQTRANS_IDLE)
		fail("08P01", "native query did not finish idle");
	emit_result(maximum, status == PGRES_TUPLES_OK ? 'T' : 'C');
	PQclear(result);
	result = NULL;
}

static uint32_t
result_bound(const unsigned char *body, uint32_t size)
{
	if (size < 4)
		fail("08P01", "helper request lacks a result bound");
	uint32_t maximum = get32(body);
	if (maximum < 32 || maximum > RESULT_MAX)
		fail("08P01", "helper request has an invalid result bound");
	return maximum;
}

static void
query(const unsigned char *body, uint32_t size)
{
	if (!connection || PQstatus(connection) != CONNECTION_OK || phase != COMMAND_MODE)
		fail("08P01", "helper query requires an idle connection");
	uint32_t maximum = result_bound(body, size);
	const unsigned char *next = body + 4;
	char *text = read_string(&next, body + size);
	if (next != body + size)
		fail("08P01", "helper query has trailing bytes");
	int64_t deadline = milliseconds() + OPERATION_MS;
	int sent = PQsendQuery(connection, text);
	free(text);
	if (!sent)
		fail("08006", "libpq query send failed");
	flush_output(deadline);
	result = next_result(deadline);
	if (result && PQresultStatus(result) == PGRES_COPY_BOTH)
	{
		unsigned char format[5];
		format[0] = PQbinaryTuples(result);
		put32(format + 1, PQnfields(result));
		PQclear(result);
		result = NULL;
		phase = COPY_BOTH_MODE;
		reply('B', format, sizeof(format));
	}
	else
		finish_result(maximum, deadline);
}

static void
receive_copy(const unsigned char *body, uint32_t size)
{
	if (!connection || phase != COPY_BOTH_MODE || size != 8)
		fail("08P01", "helper receive requires COPY BOTH");
	uint32_t maximum = get32(body);
	uint32_t timeout = get32(body + 4);
	if (!maximum || maximum > COPY_RECEIVE_MAX || timeout > OPERATION_MS)
		fail("08P01", "helper receive has invalid bounds");
	int64_t deadline = milliseconds() + timeout;
	for (;;)
	{
		int count = PQgetCopyData(connection, &copy_data, 1);
		if (count == 0)
		{
			if (!PQconsumeInput(connection))
				fail("08006", "libpq COPY input failed");
			count = PQgetCopyData(connection, &copy_data, 1);
		}
		if (count > 0)
		{
			if ((uint32_t) count > maximum)
				fail("08P01", "native COPY payload exceeds its bound");
			reply('D', copy_data, count);
			PQfreemem(copy_data);
			copy_data = NULL;
			return;
		}
		if (count == -2)
			fail("08P01", "native COPY receive failed");
		if (count == -1)
		{
			result = next_result(milliseconds() + OPERATION_MS);
			if (!result || PQresultStatus(result) != PGRES_COPY_IN)
				fail("08P01", "native COPY lacks its input completion phase");
			PQclear(result);
			result = NULL;
			phase = COPY_IN_MODE;
			reply('F', NULL, 0);
			return;
		}
		if (!poll_socket(POLLIN, deadline))
		{
			reply('I', NULL, 0);
			return;
		}
	}
}

static void
send_copy(const unsigned char *body, uint32_t size)
{
	if (!connection || phase == COMMAND_MODE || !size || size > COPY_SEND_MAX)
		fail("08P01", "helper send has invalid COPY state or size");
	int64_t deadline = milliseconds() + OPERATION_MS;
	int status;
	while ((status = PQputCopyData(connection, (const char *) body, size)) == 0)
		wait_socket(POLLOUT, deadline);
	if (status < 0)
		fail("08006", "libpq COPY output failed");
	flush_output(deadline);
	reply('A', NULL, 0);
}

static void
end_copy(const unsigned char *body, uint32_t size)
{
	if (!connection || phase != COPY_IN_MODE || size != 4)
		fail("08P01", "helper CopyDone requires the server completion phase");
	uint32_t maximum = result_bound(body, size);
	int64_t deadline = milliseconds() + OPERATION_MS;
	int status;
	while ((status = PQputCopyEnd(connection, NULL)) == 0)
		wait_socket(POLLOUT, deadline);
	if (status < 0)
		fail("08006", "libpq CopyDone failed");
	flush_output(deadline);
	result = next_result(deadline);
	if (!result || PQresultStatus(result) != PGRES_TUPLES_OK)
		fail("08P01", "native CopyDone lacks its switch row");
	finish_result(maximum, deadline);
	phase = COMMAND_MODE;
}

static void
initialize(int argc, char **argv)
{
	if (argc != 3)
		_exit(2);
	char *end;
	errno = 0;
	long parent = strtol(argv[1], &end, 10);
	if (errno || *end || parent <= 1 || parent > INT_MAX)
		_exit(2);
	errno = 0;
	session_id = strtoull(argv[2], &end, 16);
	if (errno || *end || !session_id || strlen(argv[2]) != 16)
		_exit(2);
	if (prctl(PR_SET_PDEATHSIG, SIGKILL) != 0 || getppid() != parent)
		_exit(2);
	if (close_range(3, UINT_MAX, 0) != 0)
		_exit(2);
	int quiet = open("/dev/null", O_WRONLY | O_CLOEXEC);
	if (quiet < 0 || dup2(quiet, STDERR_FILENO) < 0)
		_exit(2);
	close(quiet);
	struct rlimit memory = {MEMORY_LIMIT, MEMORY_LIMIT};
	if (setrlimit(RLIMIT_AS, &memory) != 0)
		_exit(2);
	signal(SIGPIPE, SIG_IGN);
	unsigned char hello[16];
	put32(hello, getpid());
	put32(hello + 4, parent);
	put32(hello + 8, PQlibVersion());
	put32(hello + 12, MEMORY_LIMIT);
	reply('H', hello, sizeof(hello));
}

int
main(int argc, char **argv)
{
	initialize(argc, argv);
	for (;;)
	{
		unsigned char header[HEADER_SIZE];
		int read_status = read_exact(header, sizeof(header));
		if (read_status == 0)
			break;
		if (read_status < 0)
			fail("08P01", "helper request header is incomplete");
		uint64_t next_id = get64(header + 12);
		if (memcmp(header, "PGLQ", 4) || get64(header + 4) != session_id ||
			request_id == UINT64_MAX || next_id != request_id + 1)
			fail("08P01", "helper request identity is invalid");
		request_id = next_id;
		uint32_t size = get32(header + 21);
		if (size > REQUEST_MAX)
			fail("08P01", "helper request exceeds its bound");
		request_body = malloc(size ? size : 1);
		if (!request_body)
			fail("53000", "helper memory exhausted");
		if (size && read_exact(request_body, size) != 1)
			fail("08P01", "helper request body is incomplete");
		switch (header[20])
		{
			case 'O': open_connection(request_body, size); break;
			case 'Q': query(request_body, size); break;
			case 'R': receive_copy(request_body, size); break;
			case 'S': send_copy(request_body, size); break;
			case 'E': end_copy(request_body, size); break;
			case 'X':
				if (size)
					fail("08P01", "helper close request has a body");
				reply('A', NULL, 0);
				cleanup();
				return 0;
			default: fail("08P01", "unknown helper operation");
		}
		free(request_body);
		request_body = NULL;
	}
	cleanup();
	return 0;
}
