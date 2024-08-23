#include "hello_world_util.h"
#include "ucp_util.h"

#include <ucp/api/ucp.h>

#include <string.h>    /* memset */
#include <arpa/inet.h> /* inet_addr */
#include <unistd.h>    /* getopt */
#include <stdlib.h>    /* atoi */

#define server_port 13337
#define test_string_length 16
#define IP_STRING_LEN          50
#define PORT_STRING_LEN        8

unsigned long send_recv_type = UCS_BIT(0);
ucp_context_h ucp_context;
ucp_worker_h ucp_worker;
static int connection_closed   = 1;

typedef struct ucx_server_ctx {
    volatile ucp_conn_request_h conn_request;
    ucp_listener_h              listener;
} ucx_server_ctx_t;

typedef struct test_req {
    int complete;
} test_req_t;

static void err_cb(void *arg, ucp_ep_h ep, ucs_status_t status)
{
    printf("error handling callback was invoked with status %d (%s)\n",
           status, ucs_status_string(status));
    connection_closed = 1;
}

static char* sockaddr_get_ip_str(const struct sockaddr_storage *sock_addr,
                                 char *ip_str, size_t max_size)
{
    struct sockaddr_in  addr_in;
    struct sockaddr_in6 addr_in6;

    switch (sock_addr->ss_family) {
    case AF_INET:
        memcpy(&addr_in, sock_addr, sizeof(struct sockaddr_in));
        inet_ntop(AF_INET, &addr_in.sin_addr, ip_str, max_size);
        return ip_str;
    case AF_INET6:
        memcpy(&addr_in6, sock_addr, sizeof(struct sockaddr_in6));
        inet_ntop(AF_INET6, &addr_in6.sin6_addr, ip_str, max_size);
        return ip_str;
    default:
        return "Invalid address family";
    }
}

static char* sockaddr_get_port_str(const struct sockaddr_storage *sock_addr,
                                   char *port_str, size_t max_size)
{
    struct sockaddr_in  addr_in;
    struct sockaddr_in6 addr_in6;

    switch (sock_addr->ss_family) {
    case AF_INET:
        memcpy(&addr_in, sock_addr, sizeof(struct sockaddr_in));
        snprintf(port_str, max_size, "%d", ntohs(addr_in.sin_port));
        return port_str;
    case AF_INET6:
        memcpy(&addr_in6, sock_addr, sizeof(struct sockaddr_in6));
        snprintf(port_str, max_size, "%d", ntohs(addr_in6.sin6_port));
        return port_str;
    default:
        return "Invalid address family";
    }
}

static void server_conn_handle_cb(ucp_conn_request_h conn_request, void *arg)
{
    ucx_server_ctx_t *context = arg;
    ucp_conn_request_attr_t attr;
    char ip_str[IP_STRING_LEN];
    char port_str[PORT_STRING_LEN];
    ucs_status_t status;

    attr.field_mask = UCP_CONN_REQUEST_ATTR_FIELD_CLIENT_ADDR;
    status = ucp_conn_request_query(conn_request, &attr);
    if (status == UCS_OK) {
        printf("Server received a connection request from client at address %s:%s\n",
               sockaddr_get_ip_str(&attr.client_address, ip_str, sizeof(ip_str)),
               sockaddr_get_port_str(&attr.client_address, port_str, sizeof(port_str)));
    } else if (status != UCS_ERR_UNSUPPORTED) {
        fprintf(stderr, "failed to query the connection request (%s)\n",
                ucs_status_string(status));
    }

    if (context->conn_request == NULL) {
        context->conn_request = conn_request;
    } else {
        /* The server is already handling a connection request from a client,
         * reject this new one */
        printf("Rejecting a connection request. "
               "Only one client at a time is supported.\n");
        status = ucp_listener_reject(context->listener, conn_request);
        if (status != UCS_OK) {
            fprintf(stderr, "server failed to reject a connection request: (%s)\n",
                    ucs_status_string(status));
        }
    }
}

static void stream_recv_cb(void *request, ucs_status_t status, size_t length,
                           void *user_data)
{
    //common_cb(user_data, "stream_recv_cb");
    test_req_t *ctx;

    if (user_data == NULL) {
        fprintf(stderr, "user_data passed to %s mustn't be NULL\n", "stream_recv_cb");
        return;
    }

    ctx           = user_data;
    ctx->complete = 1;
}

static void send_cb(void *request, ucs_status_t status, void *user_data)
{
    //common_cb(user_data, "stream_recv_cb");
    test_req_t *ctx;

    if (user_data == NULL) {
        fprintf(stderr, "user_data passed to %s mustn't be NULL\n", "send_cb");
        return;
    }

    ctx           = user_data;
    ctx->complete = 1;
}

static void print_iov(const ucp_dt_iov_t *iov)
{
    char *msg = alloca(test_string_length);
    mem_type_memcpy(msg, iov->buffer, test_string_length);
        printf("%s.\n", msg);
}

static ucs_status_t request_wait(ucp_worker_h ucp_worker, void *request,
                                 test_req_t *ctx)
{
    ucs_status_t status;

    /* if operation was completed immediately */
    if (request == NULL) {
        return UCS_OK;
    }

    if (UCS_PTR_IS_ERR(request)) {
        return UCS_PTR_STATUS(request);
    }

    while (ctx->complete == 0) {
        ucp_worker_progress(ucp_worker);
    }
    status = ucp_request_check_status(request);

    ucp_request_free(request);

    return status;
}

static int request_finalize(ucp_worker_h ucp_worker, test_req_t *request,
                            test_req_t *ctx, ucp_dt_iov_t *iov)
{
    int ret = 0;
    ucs_status_t status;

    status = request_wait(ucp_worker, request, ctx);
    if (status != UCS_OK) {
        fprintf(stderr, "unable to %s UCX message (%s)\n",
                "receive", ucs_status_string(status));
        ret = -1;
        goto release_iov;
    }

    /* Print the output of the first, last and every PRINT_INTERVAL iteration */
    print_iov(iov);

release_iov:
    mem_type_free(iov->buffer);
    return ret;
}

static int server_rev(ucp_worker_h ucp_worker, ucp_ep_h ep)
{
    ucp_dt_iov_t *iov = alloca(sizeof(ucp_dt_iov_t));
    ucp_request_param_t param;
    test_req_t *request;
    size_t msg_length;
    void *msg;
    test_req_t ctx;

    memset(iov, 0, sizeof(*iov));
    iov->length = test_string_length;
    iov->buffer = malloc(test_string_length);

    msg = iov->buffer;
    msg_length = iov->length;
    ctx.complete = 0;
    param.op_attr_mask = UCP_OP_ATTR_FIELD_CALLBACK |
                          UCP_OP_ATTR_FIELD_DATATYPE |
                          UCP_OP_ATTR_FIELD_USER_DATA;
    param.datatype     = ucp_dt_make_contig(1);
    param.user_data    = &ctx;

    param.op_attr_mask  |= UCP_OP_ATTR_FIELD_FLAGS;
    param.flags          = UCP_STREAM_RECV_FLAG_WAITALL;
    param.cb.recv_stream = stream_recv_cb;
    request              = ucp_stream_recv_nbx(ep, msg, msg_length,
                                                   &msg_length, &param);

    return request_finalize(ucp_worker, request, &ctx, iov);
}

static int server_send(ucp_worker_h ucp_worker, ucp_ep_h ep)
{
    ucp_dt_iov_t *iov = alloca(sizeof(ucp_dt_iov_t));
    ucp_request_param_t param;
    test_req_t *request;
    size_t msg_length;
    void *msg;
    test_req_t ctx;

    memset(iov, 0, sizeof(*iov));
    iov->length = test_string_length;
    iov->buffer = malloc(test_string_length);

    //generate_test_string(iov->buffer,iov->length);
    char *str = "helloworld!";
    memcpy(iov->buffer,str,strlen(str));
    //printf("%d\n",strlen(str));

    msg = iov->buffer;
    msg_length = iov->length;
    ctx.complete = 0;
    param.op_attr_mask = UCP_OP_ATTR_FIELD_CALLBACK |
                          UCP_OP_ATTR_FIELD_DATATYPE |
                          UCP_OP_ATTR_FIELD_USER_DATA;
    param.datatype     = ucp_dt_make_contig(1);
    param.user_data    = &ctx;

    param.cb.send = send_cb;
    request       = ucp_stream_send_nbx(ep, msg, msg_length, &param);

    return request_finalize(ucp_worker, request, &ctx, iov);
}

static int server_do_work(ucp_worker_h ucp_worker, ucp_ep_h ep,
                                 unsigned long send_recv_type, int is_server)
{
    int i, ret = 0;
    ucs_status_t status;
    //ep_close(ucp_worker, ep, UCP_EP_CLOSE_FLAG_FORCE);
    connection_closed = 0;
    ret = server_rev(ucp_worker, ep);
    if (ret != 0) {
            fprintf(stderr, "%s failed on iteration #%d\n",
                    (is_server ? "server": "client"), i + 1);
            return -1;
    }
    ret = server_send(ucp_worker, ep);
    
    if (ret != 0) {
        fprintf(stderr, "%s failed on FIN message\n",
                (is_server ? "server": "client"));
        goto out;
    }
    
    printf("%s FIN message\n", is_server ? "sent" : "received");

    /* Server waits until the client closed the connection after receiving FIN */
    while (is_server && !connection_closed) {
        ucp_worker_progress(ucp_worker);
    }

out:
    return ret;
}

int main(void)
{
    int ret;
    ucx_server_ctx_t *context = malloc(sizeof(ucx_server_ctx_t));
    ucp_listener_params_t params;
    struct sockaddr_storage *listen_addr = malloc(sizeof(struct sockaddr_storage));
    ucs_status_t status;
    char ip_str[IP_STRING_LEN];
    char port_str[PORT_STRING_LEN];
    ucp_params_t ucp_params;
    ucp_worker_params_t worker_params;
    ucp_worker_h     ucp_data_worker;
    ucp_ep_h         server_ep;
    ucp_ep_params_t ep_params;
    
    memset(&ucp_params,0,sizeof(ucp_params));
    memset(&worker_params, 0, sizeof(worker_params));

    ucp_params.field_mask = UCP_PARAM_FIELD_FEATURES | UCP_PARAM_FIELD_NAME;
    ucp_params.name       = "server";
    ucp_params.features = UCP_FEATURE_STREAM;
    status = ucp_init(&ucp_params, NULL, &ucp_context);
    //printf("%d\n",status);
    if (status != UCS_OK) {
        fprintf(stderr, "failed to ucp_init (%s)\n", ucs_status_string(status));
        ret = -1;
        return ret;
    }
    worker_params.field_mask  = UCP_WORKER_PARAM_FIELD_THREAD_MODE;
    worker_params.thread_mode = UCS_THREAD_MODE_SINGLE;
    status = ucp_worker_create(ucp_context, &worker_params, &ucp_data_worker);
    if (status != 0) {
        return status;
    }
    status = ucp_worker_create(ucp_context, &worker_params, &ucp_worker);
    if (status != 0) {
        return status;
    }
    memset(listen_addr, 0, sizeof(*listen_addr));
    struct sockaddr_in* sa_in = (struct sockaddr_in*)listen_addr;
    sa_in->sin_addr.s_addr = INADDR_ANY;
    sa_in->sin_family = AF_INET;
    sa_in->sin_port   = htons(server_port);

    context->conn_request = NULL;

    params.field_mask         = UCP_LISTENER_PARAM_FIELD_SOCK_ADDR |
                                UCP_LISTENER_PARAM_FIELD_CONN_HANDLER;
    params.sockaddr.addr      = (const struct sockaddr*)listen_addr;
    params.sockaddr.addrlen   = sizeof(*listen_addr);
    params.conn_handler.cb    = server_conn_handle_cb;
    params.conn_handler.arg   = context;

    status = ucp_listener_create(ucp_worker, &params, &context->listener);
    //printf("Waiting for connection...\n");
    if (status != UCS_OK) {
        fprintf(stderr, "failed to listen (%s)\n", ucs_status_string(status));
        return 1;
    }

    ucp_listener_attr_t attr;

    attr.field_mask = UCP_LISTENER_ATTR_FIELD_SOCKADDR;
    status = ucp_listener_query(context->listener, &attr);
    if (status != UCS_OK) {
        fprintf(stderr, "failed to query the listener (%s)\n",
                ucs_status_string(status));
        ucp_listener_destroy(context->listener);
        return status;
    }

    fprintf(stderr, "server is listening on IP %s port %s\n",
            sockaddr_get_ip_str(&attr.sockaddr, ip_str, IP_STRING_LEN),
            sockaddr_get_port_str(&attr.sockaddr, port_str, PORT_STRING_LEN));

    printf("Waiting for connection...\n");
    while (1) {
        /* Wait for the server to receive a connection request from the client.
         * If there are multiple clients for which the server's connection request
         * callback is invoked, i.e. several clients are trying to connect in
         * parallel, the server will handle only the first one and reject the rest */
        while (context->conn_request == NULL) {
            ucp_worker_progress(ucp_worker);
        }

        /* Server creates an ep to the client on the data worker.
         * This is not the worker the listener was created on.
         * The client side should have initiated the connection, leading
         * to this ep's creation */
        ep_params.field_mask      = UCP_EP_PARAM_FIELD_CONN_REQUEST|
                                    UCP_EP_PARAM_FIELD_FLAGS       |
                                    UCP_EP_PARAM_FIELD_SOCK_ADDR   |
                                    UCP_EP_PARAM_FIELD_ERR_HANDLER |
                                    UCP_EP_PARAM_FIELD_ERR_HANDLING_MODE;
        ep_params.err_mode        = UCP_ERR_HANDLING_MODE_PEER;
        ep_params.conn_request    = context->conn_request;
        ep_params.err_handler.cb  = err_cb;
        ep_params.err_handler.arg = NULL;

        status = ucp_ep_create(ucp_data_worker, &ep_params, &server_ep);
        if (status != UCS_OK) {
            fprintf(stderr, "failed to create an endpoint on the server: (%s)\n",
                ucs_status_string(status));
            return -1;
        }

        /* The server waits for all the iterations to complete before moving on
         * to the next client */
        ret = server_do_work(ucp_data_worker, server_ep, send_recv_type,
                                    1);
        if (ret != 0) {
            return -1;
        }

        /* Close the endpoint to the client */
        ep_close(ucp_data_worker, server_ep, UCP_EP_CLOSE_FLAG_FORCE);

        /* Reinitialize the server's context to be used for the next client */
        context->conn_request = NULL;

        printf("Waiting for connection...\n");
    }

    return 0;
}