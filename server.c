#define _GNU_SOURCE

#include "raft.h"
#include "server.h"

#include <alloca.h>
#include <assert.h>
#include <errno.h>
#include <fcntl.h>
#include <inttypes.h>
#include <netdb.h>
#include <poll.h>
#include <setjmp.h>
#include <signal.h>
#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <unistd.h>

#include <sys/mman.h>
#include <sys/socket.h>
#include <sys/stat.h>
#include <sys/time.h>
#include <sys/types.h>

#include <arpa/inet.h>

/*
 * Globals.
 */

static char *raft_message_type_names[4]
    = { "VOTE_CALL", "APPEND_CALL", "VOTE_RESULT", "APPEND_RESULT" };

static char *raft_state_names[3] = { "FOLLOWER", "CANDIDATE", "LEADER" };

static char *raft_server_response_names[5]
    = { "NOT_LEADER", "BECAME_LEADER", "READ_PENDING", "READ_RESULT", "COMMAND_APPLIED" };

static sigjmp_buf env;

/* Must be global so that the timeout handler can read it.
 */
static volatile sig_atomic_t current_state = RAFT_FOLLOWER;

static struct sigaction ignore_sigalrm;

static timer_t timer;

/* Declaring the persist pointer globally is admittedly ugly.
 * The motivation is that it may be modified in raft_follower
 * or raft_candidate (when they extend the heap), and these
 * updates need to propagate to the functions for the other states
 * even in the event of a timeout+siglongjmp. The only routines
 * that modify this variable are raft_main and raft_extend.
 */
static struct raft_persistent *persist;

/*
 * Error-handling routines.
 */

static void raft_fatal_error(const char *s)
{
    fprintf(stderr, "%s\n", s);
    _exit(EXIT_FAILURE);
}

static void raft_system_error(const char *s)
{
    perror(s);
    _exit(EXIT_FAILURE);
}

/*
 * Logging routines.
 */

/* We block SIGALRM while logging because fprintf is not async signal safe.
 * Ordinarily this would only be a problem when calling it from a signal
 * handler, but because the SIGALRM handler performs a nonlocal return,
 * any code that this signal could potentially interrupt is affected.
 * Since we call raft_log_message_receipt and raft_log_state while the timer
 * is running, they need to be guarded this way.
 */
static inline void block_sigalrm(struct sigaction *saved)
{
    if (sigaction(SIGALRM, &ignore_sigalrm, saved) < 0)
        raft_system_error("block_sigalrm: sigaction");
}

static inline void unblock_sigalrm(const struct sigaction *saved)
{
    if (sigaction(SIGALRM, saved, NULL) < 0)
        raft_system_error("unblock_sigalrm: sigaction");
}

static void raft_print_message(FILE *f, raft_server_id dest,
                               const struct raft_message *msg)
{
    fprintf(f, "%s %" SCNu16 "->%" SCNu16 " sender_term=%" SCNu32 " ",
            raft_message_type_names[msg->type], msg->sender, dest, msg->sender_term);
    switch (msg->type) {
    case RAFT_APPEND_CALL:
        fprintf(f, "index=%" SCNu32 " num_entries=%" SCNu16,
                msg->contents.call_contents.index,
                msg->contents.call_contents.num_entries);
        break;
    case RAFT_VOTE_RESULT:
    case RAFT_APPEND_RESULT:
        fprintf(f, "result=%s",
                msg->contents.result_contents.result ? "SUCCESS" : "FAILURE");
        break;
    default:
        break;
    }
}

static void raft_log_message_receipt(raft_server_id dest, const struct raft_message *msg)
{
    struct sigaction saved;

    block_sigalrm(&saved);
    fprintf(stderr, "IN:  ");
    raft_print_message(stderr, dest, msg);
    fprintf(stderr, "\n");
    unblock_sigalrm(&saved);
}

static void raft_log_message_send(raft_server_id dest, const struct raft_message *msg)
{
    fprintf(stderr, "OUT: ");
    raft_print_message(stderr, dest, msg);
    fprintf(stderr, "\n");
}

static void raft_log_state(void)
{
    struct sigaction saved;

    block_sigalrm(&saved);
    fprintf(stderr, "STATE: %s\n", raft_state_names[current_state]);
    unblock_sigalrm(&saved);
}

static void raft_log_client_request(raft_client_request_type type)
{
    fprintf(stderr, "IN: CLIENT %s\n", type == RAFT_READ ? "READ" : "WRITE");
}

static void raft_log_client_response(raft_server_response_type type)
{
    fprintf(stderr, "OUT: CLIENT %s\n", raft_server_response_names[type]);
}

/*
 * Client-server communication routines.
 */

static raft_client_request_type raft_receive_client_request(int client_fd, void *data)
{
    raft_client_request_type type;
    uint8_t buf[RAFT_CLIENT_DATA_SIZE];
    uint8_t *p = buf;
    size_t left;
    ssize_t got;

    if (read(client_fd, &type, sizeof(type)) < 0)
        raft_system_error("raft_receive_client_request: read");
    type = ntohs(type);

    switch (type) {
    case RAFT_READ:
        left = RAFT_READ_DATA_SIZE;
        while (left > 0) {
            if ((got = read(client_fd, p, left)) < 0)
                raft_system_error("raft_receive_client_request: read");
            left -= (size_t)got;
            p += (size_t)got;
        }

        if (data)
            memcpy(data, buf, RAFT_READ_DATA_SIZE);

        break;
    case RAFT_WRITE:
        left = RAFT_ENTRY_DATA_SIZE;
        while (left > 0) {
            if ((got = read(client_fd, p, left)) < 0)
                raft_system_error("raft_receive_client_request: read");
            left -= (size_t)got;
            p += (size_t)got;
        }

        if (data)
            memcpy(data, buf, RAFT_ENTRY_DATA_SIZE);

        break;
    }

    raft_log_client_request(type);

    return type;
}

static void raft_alert_client(int client_fd, raft_server_response_type type, void *data)
{
    uint16_t net_type = htons(type);

    if (write(client_fd, &net_type, sizeof(net_type)) < 0)
        raft_system_error("raft_alert_client: write");
    if (type == RAFT_READ_RESULT) {
        if (write(client_fd, data, RAFT_READ_RESULT_SIZE) < 0)
            raft_system_error("raft_alert_client: write");
    }

    raft_log_client_response(type);
}

/* A wrapper around the client-provided apply_command callback.
 */
static void raft_ratify_entry(const struct raft_entry *ent)
{
    if (ent->type == RAFT_ENTRY_NORMAL)
        apply_command(ent->data);
}

/*
 * Server-server communication routines.
 */

/* Serialization of the raft_message struct to a portable wire format.
 * We pack all members into a buffer (no padding) and convert integers
 * to network byte order. Note that we need to use memcpy even for
 * scalar values since alignment requirements may not be satisfied.
 */
static void raft_send_message(const struct raft_context *ctx, raft_server_id dest,
                              const struct raft_message *msg)
{
    static uint8_t buf[RAFT_MAX_MESSAGE_SIZE];
    uint8_t *p = buf;
    uint16_t ns;
    uint32_t nl;
    const struct raft_call_contents *call_contents = NULL;
    const struct raft_result_contents *result_contents = NULL;
    uint16_t k;

    memset(buf, 0, RAFT_MAX_MESSAGE_SIZE);

    ns = htons(msg->type);
    memcpy(p, &ns, sizeof(ns));
    p += sizeof(ns);

    ns = htons(msg->sender);
    memcpy(p, &ns, sizeof(ns));
    p += sizeof(ns);

    nl = htonl(msg->sender_term);
    memcpy(p, &nl, sizeof(nl));
    p += sizeof(nl);

    if (msg->type == RAFT_VOTE_CALL || msg->type == RAFT_APPEND_CALL) {
        call_contents = &(msg->contents.call_contents);

        nl = htonl(call_contents->index);
        memcpy(p, &nl, sizeof(nl));
        p += sizeof(nl);

        nl = htonl(call_contents->term);
        memcpy(p, &nl, sizeof(nl));
        p += sizeof(nl);
    }
    if (msg->type == RAFT_APPEND_CALL) {
        nl = htonl(call_contents->commit);
        memcpy(p, &nl, sizeof(nl));
        p += sizeof(nl);

        ns = htons(call_contents->num_entries);
        memcpy(p, &ns, sizeof(ns));
        p += sizeof(ns);

        for (k = 0; k < call_contents->num_entries; ++k) {
            ns = htons(call_contents->entries[k].type);
            memcpy(p, &ns, sizeof(ns));
            p += sizeof(ns);
            nl = htonl(call_contents->entries[k].term_added);
            memcpy(p, &nl, sizeof(nl));
            p += sizeof(nl);
            memcpy(p, call_contents->entries[k].data, RAFT_ENTRY_DATA_SIZE);
            p += RAFT_ENTRY_DATA_SIZE;
        }
    }

    if (msg->type == RAFT_VOTE_RESULT || msg->type == RAFT_APPEND_RESULT) {
        result_contents = &(msg->contents.result_contents);

        ns = htons(result_contents->result);
        memcpy(p, &ns, sizeof(ns));
        p += sizeof(ns);
    }
    if (msg->type == RAFT_APPEND_RESULT) {
        nl = htonl(result_contents->index);
        memcpy(p, &nl, sizeof(nl));
        p += sizeof(nl);

        ns = htons(result_contents->num_entries);
        memcpy(p, &ns, sizeof(ns));
    }

    if (sendto(ctx->server_sock, buf, RAFT_MAX_MESSAGE_SIZE, 0,
               (struct sockaddr *)&(ctx->server_addrs[dest]), sizeof(struct sockaddr_in))
        < 0)
        raft_system_error("raft_send_message: sendto");

    raft_log_message_send(dest, msg);
}

/* Deserialization of the wire format; the inverse of the above.
 */
static void raft_receive_message(const struct raft_context *ctx, struct raft_message *msg)
{
    static uint8_t buf[RAFT_MAX_MESSAGE_SIZE];
    uint8_t *p = buf;
    uint16_t ns;
    uint32_t nl;
    struct raft_call_contents *call_contents = NULL;
    struct raft_result_contents *result_contents = NULL;
    uint16_t k;

    memset(buf, 0, RAFT_MAX_MESSAGE_SIZE);
    if (recv(ctx->server_sock, buf, RAFT_MAX_MESSAGE_SIZE, 0) < 0)
        raft_system_error("raft_receive_message: recv");

    memcpy(&ns, p, sizeof(ns));
    msg->type = ntohs(ns);
    p += sizeof(ns);

    memcpy(&ns, p, sizeof(ns));
    msg->sender = ntohs(ns);
    p += sizeof(ns);

    memcpy(&nl, p, sizeof(nl));
    msg->sender_term = ntohl(nl);
    p += sizeof(nl);

    if (msg->type == RAFT_VOTE_CALL || msg->type == RAFT_APPEND_CALL) {
        call_contents = &(msg->contents.call_contents);

        memcpy(&nl, p, sizeof(nl));
        call_contents->index = ntohl(nl);
        p += sizeof(nl);

        memcpy(&nl, p, sizeof(nl));
        call_contents->term = ntohl(nl);
        p += sizeof(nl);
    }
    if (msg->type == RAFT_APPEND_CALL) {
        memcpy(&nl, p, sizeof(nl));
        call_contents->commit = ntohl(nl);
        p += sizeof(nl);

        memcpy(&ns, p, sizeof(ns));
        call_contents->num_entries = ntohs(ns);
        p += sizeof(ns);

        for (k = 0; k < call_contents->num_entries; ++k) {
            memcpy(&ns, p, sizeof(ns));
            call_contents->entries[k].type = ntohs(ns);
            p += sizeof(ns);
            memcpy(&nl, p, sizeof(nl));
            call_contents->entries[k].term_added = ntohl(nl);
            p += sizeof(nl);
            memcpy(&(call_contents->entries[k].data), p, RAFT_ENTRY_DATA_SIZE);
            p += RAFT_ENTRY_DATA_SIZE;
        }
    }

    if (msg->type == RAFT_VOTE_RESULT || msg->type == RAFT_APPEND_RESULT) {
        result_contents = &(msg->contents.result_contents);

        memcpy(&ns, p, sizeof(ns));
        result_contents->result = ntohs(ns);
        p += sizeof(ns);
    }
    if (msg->type == RAFT_APPEND_RESULT) {
        memcpy(&nl, p, sizeof(nl));
        result_contents->index = ntohl(nl);
        p += sizeof(nl);

        memcpy(&ns, p, sizeof(ns));
        result_contents->num_entries = ntohs(ns);
    }

    raft_log_message_receipt(ctx->my_id, msg);
}

/* This and the next few functions are simple helpers that take
 * care of writing all the necessary fields of the raft_message struct
 * before shipping it to raft_send_message.
 */
static void raft_respond_vote(const struct raft_context *ctx, raft_server_id dest,
                              raft_bool result)
{
    struct raft_message msg = { .type = RAFT_VOTE_RESULT,
                                .sender = ctx->my_id,
                                .sender_term = persist->current_term,
                                .contents = { .result_contents = { .result = result } } };

    raft_send_message(ctx, dest, &msg);
}

static void raft_respond_append(const struct raft_context *ctx,
                                const struct raft_message *req, raft_bool result)
{
    struct raft_message msg
        = { .type = RAFT_APPEND_RESULT,
            .sender = ctx->my_id,
            .sender_term = persist->current_term,
            .contents
            = { .result_contents
                = { .result = result,
                    .index = req->contents.call_contents.index,
                    .num_entries = req->contents.call_contents.num_entries } } };

    raft_send_message(ctx, req->sender, &msg);
}

static void raft_request_append(const struct raft_context *ctx,
                                const struct raft_transient *trans, raft_server_id dest,
                                uint16_t num_entries, raft_index start)
{
    assert(start > 0);
    struct raft_message msg
        = { .type = RAFT_APPEND_CALL,
            .sender = ctx->my_id,
            .sender_term = persist->current_term,
            .contents = { .call_contents = { .index = start - 1,
                                             .term = persist->log[start - 1].term_added,
                                             .commit = trans->commit_index,
                                             .num_entries = num_entries } } };

    assert(num_entries <= RAFT_MAX_APPEND_ENTRIES);
    memcpy(msg.contents.call_contents.entries, &(persist->log[start]),
           num_entries * sizeof(struct raft_entry));

    raft_send_message(ctx, dest, &msg);
}

/* This routine is called by leaders to send a round of append messages
 * to every other server. Some of these messages will contain no entries
 * (when the corresponding server has all the leader's entries), in which
 * case they function as heartbeats.
 */
static void raft_request_all_appends(const struct raft_context *ctx,
                                     const struct raft_transient *trans,
                                     const raft_index *next_indices)
{
    raft_server_id i;

    for (i = 1; i <= ctx->num_servers; ++i) {
        uint16_t num_send;

        if (i == ctx->my_id)
            continue;

        assert(persist->last_index + 1 >= next_indices[i]); // sanity check
        num_send = MIN((uint16_t)(persist->last_index + 1 - next_indices[i]),
                       RAFT_MAX_APPEND_ENTRIES);
        raft_request_append(ctx, trans, i, num_send, next_indices[i]);
    }
}

/*
 * Timer-handling routines.
 */

static void raft_reset_timer(void)
{
    long ns = RAFT_ELECTION_BASE_NS + (unsigned long)random() % RAFT_ELECTION_RANGE_NS;

    // We use a "non-repeating" timer since a new random timeout is used
    // every time the timer is reset.
    struct itimerspec spec = { .it_value = { .tv_sec = 0, .tv_nsec = ns },
                               .it_interval = { .tv_sec = 0, .tv_nsec = 0 } };

    if (timer_settime(timer, 0, &spec, NULL) < 0)
        raft_system_error("raft_reset_timer: timer_settime");
}

static void raft_pause_timer(struct itimerspec *saved)
{
    static struct itimerspec zeroed = { .it_value = { .tv_sec = 0, .tv_nsec = 0 },
                                        .it_interval = { .tv_sec = 0, .tv_nsec = 0 } };

    if (timer_settime(timer, 0, &zeroed, saved) < 0)
        raft_system_error("raft_pause_timer: timer_settime");
}

static void raft_restore_timer(const struct itimerspec *saved)
{
    if (timer_settime(timer, 0, saved, NULL) < 0)
        raft_system_error("raft_restore_timer: timer_settime");
}

/*
 * Routines to manage persistence.
 */

static void raft_sync()
{
    if (msync(persist, persist->max_size, MS_SYNC) < 0)
        raft_system_error("raft_sync: msync");
}

static void raft_extend(int persist_fd)
{
    // This routine jointly extends the size of the underlying file
    // and of the mapped region. It would probably be more efficient to map
    // a huge memory area at the start and then only extend the file in
    // the common case.
    size_t prev_size = persist->max_size;
    size_t new_size
        = prev_size + RAFT_PERSIST_EXTEND_PAGES * (size_t)sysconf(_SC_PAGESIZE);

    // Extend the underlying file.
    if (ftruncate(persist_fd, (off_t)new_size) < 0)
        raft_system_error("raft_extend: ftruncate");

    persist = mremap(persist, prev_size, new_size, MREMAP_MAYMOVE);
    if (persist == MAP_FAILED)
        raft_system_error("raft_extend: mremap");
    persist->max_size = new_size;
}

/*
 * Core routines.
 */

static void raft_timeout_handler(int sig)
{
    (void)sig;
    assert(current_state == RAFT_FOLLOWER
           || current_state == RAFT_CANDIDATE); // sanity check
    current_state = RAFT_CANDIDATE;
    siglongjmp(env, 1);
}

static void raft_follower(struct raft_context *ctx, int client_fd, int persist_fd,
                          struct raft_transient *trans)
{
    struct pollfd poll_fds[2] = { { .fd = client_fd, .events = POLLIN },
                                  { .fd = ctx->server_sock, .events = POLLIN } };

    raft_reset_timer();

    while (true) {
        int ret;
        struct raft_message msg;
        struct itimerspec saved;
        struct raft_call_contents *contents;
        raft_term last_term;
        raft_index j;

        // Use poll to wait on data from either a client or another server.
        while ((ret = poll(poll_fds, 2, -1)) < 0 && errno == EINTR)
            ;

        if (ret < 0)
            raft_system_error("raft_follower: poll");

        // Followers refuse to satisfy client requests.
        if (poll_fds[0].revents & POLLIN) {
            raft_pause_timer(&saved);
            (void)raft_receive_client_request(client_fd, NULL);
            raft_alert_client(client_fd, RAFT_NOT_LEADER, NULL);
            raft_restore_timer(&saved);
        }

        // Process a server message if one is present.
        if (poll_fds[1].revents & POLLIN) {
            raft_receive_message(ctx, &msg);
            raft_pause_timer(&saved);

            // Candidates and leaders revert immediately to follower state
            // upon seeing a message with a newer term. If we're a follower already,
            // we might as well continue to process the current message after
            // updating our term.
            if (msg.sender_term > persist->current_term) {
                persist->current_term = msg.sender_term;
                raft_sync();
                persist->voted_for = 0;
            }

            if (msg.type == RAFT_VOTE_CALL) {
                contents = &(msg.contents.call_contents);
                last_term = persist->log[persist->last_index].term_added;

                // Check whether the candidate's log is at least as up to date as ours,
                // to prevent an "ignorant" server that's missing entries that are present
                // on a majority of servers from becoming leader and erasing these
                // entries.
                if (msg.sender_term == persist->current_term
                    && (!persist->voted_for || persist->voted_for == msg.sender)
                    && (contents->term > last_term
                        || (contents->term == last_term
                            && contents->index >= persist->last_index))) {
                    persist->voted_for = msg.sender;

                    raft_sync();
                    raft_respond_vote(ctx, msg.sender, RAFT_SUCCESS);
                    raft_reset_timer();
                } else {
                    raft_sync();
                    raft_respond_vote(ctx, msg.sender, RAFT_FAILURE);
                    raft_restore_timer(&saved);
                }
            } else if (msg.type == RAFT_APPEND_CALL) {
                contents = &(msg.contents.call_contents);

                // Check whether the append request is compatible with our log.
                if (msg.sender_term == persist->current_term) {
                    if (contents->index <= persist->last_index
                        && contents->term == persist->log[contents->index].term_added) {
                        // Extend the log if necessary.
                        if (RAFT_PERSIST_SIZE_UPTO(contents->index + 1
                                                   + contents->num_entries)
                            > persist->max_size)
                            raft_extend(persist_fd);
                        // Update our log. We may end up with fewer entries than
                        // before.
                        memcpy(&(persist->log[contents->index + 1]), contents->entries,
                               contents->num_entries * sizeof(struct raft_entry));
                        raft_sync();
                        persist->last_index = contents->index + contents->num_entries;

                        // Update our view of what's committed, and apply any newly
                        // committed entries to the state machine.
                        if (contents->commit > trans->commit_index)
                            trans->commit_index
                                = MIN(contents->commit, persist->last_index);
                        for (j = trans->applied_index + 1; j <= trans->commit_index;
                             ++j) {
                            raft_ratify_entry(&(persist->log[j]));
                        }
                        trans->applied_index = trans->commit_index;

                        raft_sync();
                        raft_respond_append(ctx, &msg, RAFT_SUCCESS);
                    } else {
                        raft_sync();
                        raft_respond_append(ctx, &msg, RAFT_FAILURE);
                    }

                    // Reset the timer whenever we see an append request from the current
                    // term, even if it was unsuccessful, since this means we're still
                    // in touch with the leader.
                    raft_reset_timer();
                } else {
                    raft_sync();
                    raft_respond_append(ctx, &msg, RAFT_FAILURE);
                    raft_restore_timer(&saved);
                }
            } else {
                // Followers don't send call messages, so any result message we receive
                // must be left over from a previous term.
                raft_restore_timer(&saved);
            }
        }
    }
}

static void raft_candidate(struct raft_context *ctx, int client_fd, int persist_fd,
                           struct raft_transient *trans)
{
    (void)persist_fd;
    (void)trans;

    bool *received_from;
    unsigned votes_received;
    raft_server_id i;
    struct pollfd poll_fds[2] = { { .fd = client_fd, .events = POLLIN },
                                  { .fd = ctx->server_sock, .events = POLLIN } };

    ++(persist->current_term);
    raft_sync();
    persist->voted_for = ctx->my_id;

    raft_reset_timer();

    // I use alloca here to avoid either declaring a variable-length array
    // (apparently considered poor form) or calling malloc (and thus leaking
    // memory if the candidate times out).
    received_from = alloca((ctx->num_servers + 1) * sizeof(bool));
    for (i = 1; i <= ctx->num_servers; ++i)
        received_from[i] = false;
    received_from[ctx->my_id] = true;
    votes_received = 1;

    // Contents of the vote request we'll send to other servers.
    struct raft_message req
        = { .type = RAFT_VOTE_CALL,
            .sender = ctx->my_id,
            .sender_term = persist->current_term,
            .contents = { .call_contents
                          = { .index = persist->last_index,
                              .term = persist->log[persist->last_index].term_added } } };

    // Initial round of vote requests.
    for (i = 1; i <= ctx->num_servers; ++i) {
        if (i != ctx->my_id)
            raft_send_message(ctx, i, &req);
    }

    while (true) {
        int ret;
        struct raft_message msg;
        struct itimerspec saved;

        while ((ret = poll(poll_fds, 2, RAFT_VOTE_TIMEOUT_MS)) <= 0) {
            if (ret < 0) {
                if (errno == EINTR)
                    continue;
                else
                    raft_system_error("raft_candidate: poll");
            }

            // Resend outstanding vote requests, which may have been lost.
            for (i = 1; i <= ctx->num_servers; ++i) {
                if (!received_from[i])
                    raft_send_message(ctx, i, &req);
            }
        }

        if (poll_fds[0].revents & POLLIN) {
            raft_pause_timer(&saved);
            (void)raft_receive_client_request(client_fd, NULL);
            raft_alert_client(client_fd, RAFT_NOT_LEADER, NULL);
            raft_restore_timer(&saved);
        }

        if (poll_fds[1].revents & POLLIN) {
            raft_receive_message(ctx, &msg);
            raft_pause_timer(&saved); // note that candidates never reset their timers

            // Revert to follower without processing the message further if we
            // see a term greater than our own. The message will eventually be
            // resent and we can process it from follower state. This way, we don't
            // have to replicate the full follower logic in this function.
            if (msg.sender_term > persist->current_term) {
                persist->current_term = msg.sender_term;
                raft_sync();
                persist->voted_for = 0;
                current_state = RAFT_FOLLOWER;
                break; // from while loop; exit raft_candidate, return to switch
            }

            if (msg.type == RAFT_VOTE_CALL) {
                // Candidates never grant their vote.
                raft_sync();
                raft_respond_vote(ctx, msg.sender, RAFT_FAILURE);
                raft_restore_timer(&saved);
            } else if (msg.type == RAFT_APPEND_CALL) {
                if (msg.sender_term == persist->current_term) {
                    // Another candidate has become leader for this term: defer
                    // to them.
                    current_state = RAFT_FOLLOWER;
                    break;
                } else {
                    raft_sync();
                    raft_respond_append(ctx, &msg, RAFT_FAILURE);
                    raft_restore_timer(&saved);
                }
            } else if (msg.type == RAFT_VOTE_RESULT
                       && msg.sender_term == persist->current_term) {
                if (msg.contents.result_contents.result && !received_from[msg.sender])
                    ++votes_received;
                received_from[msg.sender] = true;
                if (2 * votes_received > ctx->num_servers) {
                    // Majority of votes received.
                    current_state = RAFT_LEADER;
                    break;
                } else {
                    raft_restore_timer(&saved);
                }
            } else {
                raft_restore_timer(&saved);
            }
        }
    }
}

static void raft_leader(struct raft_context *ctx, int client_fd, int persist_fd,
                        struct raft_transient *trans)
{
    raft_index *next_indices, *matched_indices;
    raft_server_id i;
    struct pollfd poll_fds[2] = { { .fd = client_fd, .events = POLLIN },
                                  { .fd = ctx->server_sock, .events = POLLIN } };
    bool committed_noop = false, reader_waiting = false;
    unsigned num_heard = 1;
    bool *heard_from;
    uint8_t delayed_read[RAFT_READ_DATA_SIZE];

    next_indices = alloca((ctx->num_servers + 1) * sizeof(raft_index));
    matched_indices = alloca((ctx->num_servers + 1) * sizeof(raft_index));
    heard_from = alloca((ctx->num_servers + 1) * sizeof(bool));
    for (i = 1; i <= ctx->num_servers; ++i) {
        // The initial setting of next_indices ensures we won't send entries
        // to other servers until we get a new one from a client or find that
        // an append request failed.
        next_indices[i] = persist->last_index + 1;
        // The initial setting of matched_indices ensures that we don't commit
        // any entries that aren't known to be replicated on a majority of servers.
        matched_indices[i] = 0;
    }
    matched_indices[ctx->my_id] = persist->last_index;

    raft_alert_client(client_fd, RAFT_BECAME_LEADER, NULL);

    // Append and try to commit a no-op entry at the beginning of the term.
    // This will ensure in the process than any entries in our log from previous
    // terms are committed too; it's also required to support read-only client
    // requests.
    if (RAFT_PERSIST_SIZE_UPTO(persist->last_index + 2) > persist->max_size)
        raft_extend(persist_fd);
    persist->log[persist->last_index + 1].type = RAFT_ENTRY_NOOP;
    persist->log[persist->last_index + 1].term_added = persist->current_term;
    raft_sync();
    ++(persist->last_index);
    ++(matched_indices[ctx->my_id]);

    while (true) {
        int ret;
        uint8_t data[RAFT_CLIENT_DATA_SIZE];
        uint8_t read_res[RAFT_READ_RESULT_SIZE];
        raft_client_request_type type;
        struct raft_message msg;
        struct raft_result_contents *contents;
        raft_index old_next, old_matched, old_commit, j, jj;
        unsigned replicas;

        while ((ret = poll(poll_fds, 2, RAFT_LEADER_TIMEOUT_MS)) <= 0) {
            if (ret < 0) {
                if (errno == EINTR)
                    continue;
                else
                    raft_system_error("raft_leader: poll");
            }

            // Resend outstanding append requests, plus heartbeats to followers
            // that appear to be up to date.
            raft_request_all_appends(ctx, trans, next_indices);
        }

        if (poll_fds[0].revents & POLLIN) {
            type = raft_receive_client_request(client_fd, data);

            if (type == RAFT_READ) {
                if (reader_waiting) {
                    raft_alert_client(client_fd, RAFT_READ_PENDING, NULL);
                } else {
                    reader_waiting = true;
                    num_heard = 1;
                    for (i = 1; i <= ctx->num_servers; ++i)
                        heard_from[i] = false;
                    heard_from[ctx->my_id] = true;
                    memcpy(delayed_read, data, RAFT_READ_DATA_SIZE);
                }
            } else if (type == RAFT_WRITE) {
                if (RAFT_PERSIST_SIZE_UPTO(persist->last_index + 2) > persist->max_size)
                    raft_extend(persist_fd);
                persist->log[persist->last_index + 1].type = RAFT_ENTRY_NORMAL;
                persist->log[persist->last_index + 1].term_added = persist->current_term;
                memcpy(&(persist->log[persist->last_index + 1].data), data,
                       RAFT_ENTRY_DATA_SIZE);
                raft_sync();
                ++(persist->last_index);
                ++(matched_indices[ctx->my_id]);

                // Request appends at least once even if we don't time out.
                raft_request_all_appends(ctx, trans, next_indices);
            }
        }

        if (poll_fds[1].revents & POLLIN) {
            raft_receive_message(ctx, &msg);

            if (msg.sender_term > persist->current_term) {
                persist->current_term = msg.sender_term;
                raft_sync();
                persist->voted_for = 0;
                current_state = RAFT_FOLLOWER;
                break;
            }

            if (msg.type == RAFT_VOTE_CALL) {
                raft_sync();
                raft_respond_vote(ctx, msg.sender, RAFT_FAILURE);
            } else if (msg.type == RAFT_APPEND_CALL) {
                raft_sync();
                raft_respond_append(ctx, &msg, RAFT_FAILURE);
            } else if (msg.type == RAFT_APPEND_RESULT
                       && msg.sender_term == persist->current_term) {
                contents = &(msg.contents.result_contents);
                old_next = next_indices[msg.sender];
                old_matched = matched_indices[msg.sender];
                old_commit = trans->commit_index;

                if (contents->result == RAFT_SUCCESS) {
                    // Update our view of which entries are replicated where.
                    // The MAXes here are to handle the possibility that this response
                    // corresponds to entries that we've already marked as replicated on
                    // the sending server (possible due to reordering and resending).
                    next_indices[msg.sender]
                        = MAX(old_next, contents->index + 1 + contents->num_entries);
                    matched_indices[msg.sender]
                        = MAX(old_matched, contents->index + contents->num_entries);

                    // Count down from the newest matched index to the known-committed
                    // index, looking for entries that have achieved majority replication.
                    // This loop may degenerate (correctly) if the response didn't
                    // indicate replication of anything that isn't already committed.
                    // Note how we only apply this treatment to entries from our current
                    // term--this is required for correctness (see the paper for details).
                    // Entries from previous terms will be committed as soon as any entry
                    // from the current term is committed, including the no-op from
                    // before.
                    for (j = matched_indices[msg.sender]; j > old_commit
                         && persist->log[j].term_added == persist->current_term;
                         --j) {
                        replicas = 0;
                        for (i = 1; i <= ctx->num_servers; ++i) {
                            if (matched_indices[i] >= j)
                                ++replicas;
                        }
                        if (2 * replicas > ctx->num_servers) {
                            // Majority replication achieved for this entry and hence for
                            // all entries preceding it in the log. (This is why we count
                            // down in the loop above.)
                            trans->commit_index = j;
                            // Apply the newly committed entries to our state machine.
                            // Also alert our client for entries from the current term.
                            for (jj = old_commit + 1; jj <= j; ++jj) {
                                raft_ratify_entry(&(persist->log[jj]));
                                if (persist->log[jj].type == RAFT_ENTRY_NORMAL
                                    && persist->log[jj].term_added
                                        == persist->current_term)
                                    raft_alert_client(client_fd, RAFT_COMMAND_APPLIED,
                                                      NULL);
                            }
                            trans->applied_index = j;

                            // Now we've committed an entry from our term, and in
                            // particular we've committed the special no-op entry.
                            if (!committed_noop) {
                                committed_noop = true;
                                if (reader_waiting && 2 * num_heard > ctx->num_servers) {
                                    resolve_read(delayed_read, read_res);
                                    raft_alert_client(client_fd, RAFT_READ_RESULT,
                                                      read_res);
                                    reader_waiting = false;
                                }
                            }

                            break; // from for loop
                        }
                    }
                } else {
                    // An append request failed. Again, we may have already handled
                    // a logically "later" response, so instead of just decrementing
                    // the next index for the sending server we examine the index stated
                    // in the response itself, using this only if it would actually
                    // decrease the next index.
                    assert(contents->index > 0);
                    next_indices[msg.sender]
                        = MIN(contents->index, next_indices[msg.sender]);
                }

                // In any case, if we receive an append result from the current term
                // we've heard from the sending server.
                if (!heard_from[msg.sender]) {
                    heard_from[msg.sender] = true;
                    ++num_heard;
                    if (reader_waiting && 2 * num_heard > ctx->num_servers
                        && committed_noop) {
                        resolve_read(delayed_read, read_res);
                        raft_alert_client(client_fd, RAFT_READ_RESULT, read_res);
                        reader_waiting = false;
                    }
                }
            }
        }
    }
}

static void raft_server(struct raft_context *ctx, int client_fd, int persist_fd)
{
    struct sigaction act;
    struct raft_transient trans = { .commit_index = 0, .applied_index = 0 };

    sigemptyset(&(act.sa_mask));
    act.sa_flags = SA_RESTART;
    act.sa_handler = raft_timeout_handler;
    if (sigaction(SIGALRM, &act, NULL) < 0)
        raft_system_error("raft_server: sigaction");

    sigemptyset(&(ignore_sigalrm.sa_mask));
    ignore_sigalrm.sa_flags = 0;
    ignore_sigalrm.sa_handler = SIG_IGN;

    if (timer_create(CLOCK_MONOTONIC, NULL, &timer) < 0)
        raft_system_error("raft_server: timer_create");

    // Timed-out followers and candidates end up jumping here and fall back
    // into the switch statement.
    sigsetjmp(env, 1);
    while (true) {
        raft_log_state();
        switch (current_state) {
        case RAFT_FOLLOWER:
            raft_follower(ctx, client_fd, persist_fd, &trans);
            break;
        case RAFT_CANDIDATE:
            raft_candidate(ctx, client_fd, persist_fd, &trans);
            break;
        case RAFT_LEADER:
            raft_leader(ctx, client_fd, persist_fd, &trans);
            break;
        }
    }
}

/*
 * Setup routines.
 */
static void raft_init_context(raft_server_id my_id, FILE *config,
                              struct raft_context *ctx)
{
    uint16_t port;
    char port_str[NI_MAXSERV];
    raft_server_id i;

    ctx->my_id = my_id;

    if ((ctx->server_sock = socket(AF_INET, SOCK_DGRAM, 0)) < 0)
        raft_system_error("raft_init_context: socket");
    if (fscanf(config, "%" SCNu16 " ", &port) == EOF)
        raft_fatal_error("bad format for port number");
    snprintf(port_str, NI_MAXSERV, "%" SCNu16, port);
    struct sockaddr_in addr = { .sin_family = AF_INET,
                                .sin_addr = { .s_addr = htonl(INADDR_ANY) },
                                .sin_port = htons(port) };
    if (bind(ctx->server_sock, (struct sockaddr *)&addr, sizeof(addr)) < 0)
        raft_system_error("raft_init_context: bind");

    if (fscanf(config, "%zu ", &(ctx->num_servers)) == EOF)
        raft_fatal_error("bad format for number of servers or server ID");
    if (ctx->num_servers <= 1)
        raft_fatal_error("number of servers must be > 1");
    if (ctx->my_id == 0 || ctx->my_id > ctx->num_servers)
        raft_fatal_error("server ID must be between 1 and number of servers");

    ctx->server_addrs = calloc(ctx->num_servers + 1, sizeof(struct sockaddr_in));
    for (i = 1; i <= ctx->num_servers; ++i) {
        char *line = NULL;
        size_t n = 0;
        struct addrinfo hints = { .ai_family = AF_INET,
                                  .ai_socktype = SOCK_DGRAM,
                                  .ai_protocol = 0,
                                  .ai_flags = 0 };
        struct addrinfo *result;

        if (getline(&line, &n, config) < 0)
            raft_fatal_error("bad format for server address");
        strtok(line, "\n");
        if (i != ctx->my_id) {
            if (getaddrinfo(line, port_str, &hints, &result) != 0)
                raft_fatal_error("getaddrinfo failed");
            memcpy(&(ctx->server_addrs[i]), result->ai_addr, result->ai_addrlen);
            freeaddrinfo(result);
        }
        free(line);
    }
}

void raft_main(enum raft_mode mode, unsigned my_id, const char *config_path,
               const char *persist_path, int client_fd)
{
    FILE *config;
    struct raft_context ctx;
    int persist_fd;
    size_t init_persist_size, max_persist_size;

    if (!(config = fopen(config_path, "r")))
        raft_system_error("raft_main: fopen");
    raft_init_context((raft_server_id)my_id, config, &ctx);
    fclose(config);

    if (!persist_path) {
        persist_fd = fileno(tmpfile()); // ""persistence""
    } else if (mode == RAFT_RECOVER_MODE) {
        persist_fd = open(persist_path, O_RDWR | O_SYNC);
        if (persist_fd < 0)
            raft_system_error("raft_main: open");
    } else {
        persist_fd
            = open(persist_path, O_RDWR | O_CREAT | O_EXCL | O_SYNC, S_IRUSR | S_IWUSR);
        if (persist_fd < 0)
            raft_system_error("raft_main: open");
    }

    init_persist_size = RAFT_PERSIST_INIT_PAGES * (size_t)sysconf(_SC_PAGESIZE);
    if (mode == RAFT_NORMAL_MODE) {
        if (ftruncate(persist_fd, (off_t)init_persist_size) < 0)
            raft_system_error("raft_main: ftruncate");
    }
    persist = (struct raft_persistent *)mmap(
        NULL, init_persist_size, PROT_READ | PROT_WRITE, MAP_SHARED, persist_fd, 0);
    if (persist == MAP_FAILED)
        raft_system_error("raft_main: mmap");

    if (mode == RAFT_NORMAL_MODE) {
        persist->current_term = 0;
        persist->voted_for = 0;
        persist->last_index = 0;
        persist->max_size = init_persist_size;
        // The first log entry is a dummy entry--it's there to ensure that the logs
        // of two servers always have a nonempty common prefix. Here it's tagged
        // as a no-op to prevent the server from trying to apply it to the state
        // machine.
        persist->log[0].type = RAFT_ENTRY_NOOP;
        persist->log[0].term_added = 0;
    } else {
        // Check whether the log we're recovering from was expanded, and if so remap
        // the memory appropriately.
        max_persist_size = persist->max_size;
        if (max_persist_size > init_persist_size) {
            persist
                = mremap(persist, init_persist_size, max_persist_size, MREMAP_MAYMOVE);
            if (persist == MAP_FAILED)
                raft_system_error("raft_main: mremap");
        }
    }

    raft_server(&ctx, client_fd, persist_fd);
}
