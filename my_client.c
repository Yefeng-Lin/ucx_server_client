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

static void common_cb(void *user_data, const char *type_str)
{
    test_req_t *ctx;

    if (user_data == NULL) {
        fprintf(stderr, "user_data passed to %s mustn't be NULL\n", type_str);
        return;
    }

    ctx           = user_data;
    ctx->complete = 1;
}

static void send_cb(void *request, ucs_status_t status, void *user_data)
{
    common_cb(user_data, "send_cb");
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

static void print_iov(const ucp_dt_iov_t *iov)
{
    char *msg = alloca(test_string_length);
    //size_t idx;

    mem_type_memcpy(msg, iov->buffer, test_string_length);
    printf("%s.\n", msg);
}

static int request_finalize(ucp_worker_h ucp_worker, test_req_t *request,
                            test_req_t *ctx, ucp_dt_iov_t *iov)
{
    int ret = 0;
    ucs_status_t status;

    status = request_wait(ucp_worker, request, ctx);
    if (status != UCS_OK) {
        fprintf(stderr, "unable to %s UCX message (%s)\n",
                    "send", ucs_status_string(status));
        ret = -1;
        goto release_iov;
    }

    print_iov(iov);

release_iov:
    mem_type_free(iov->buffer);
    return ret;
}

static int client_send(ucp_worker_h ucp_worker, ucp_ep_h ep)
{
    ucp_dt_iov_t *iov = alloca(sizeof(ucp_dt_iov_t));
    ucp_request_param_t param;
    test_req_t *request;
    size_t msg_length;
    void *msg;
    test_req_t ctx;

    memset(iov, 0, sizeof(*iov));
    iov->length = test_string_length;
    iov->buffer = mem_type_malloc(test_string_length);
    generate_test_string(iov->buffer,iov->length);

    msg = iov->buffer;
    msg_length = iov->length;

    ctx.complete       = 0;
    param.op_attr_mask = UCP_OP_ATTR_FIELD_CALLBACK |
                          UCP_OP_ATTR_FIELD_DATATYPE |
                          UCP_OP_ATTR_FIELD_USER_DATA;
    param.datatype     = ucp_dt_make_contig(1);
    param.user_data    = &ctx;
    param.cb.send = send_cb;
    request       = ucp_stream_send_nbx(ep, msg, msg_length, &param);
    return request_finalize(ucp_worker, request, &ctx, iov);
}

static void stream_recv_cb(void *request, ucs_status_t status, size_t length,
                           void *user_data)
{
    common_cb(user_data, "stream_recv_cb");
}

static int client_recv(ucp_worker_h ucp_worker, ucp_ep_h ep)
{
    ucp_dt_iov_t *iov = alloca(sizeof(ucp_dt_iov_t));
    ucp_request_param_t param;
    test_req_t *request;
    size_t msg_length;
    void *msg;
    test_req_t ctx;

    memset(iov, 0, sizeof(*iov));
    iov->length = test_string_length;
    iov->buffer = mem_type_malloc(test_string_length);
    //generate_test_string(iov->buffer,iov->length);

    msg = iov->buffer;
    msg_length = iov->length;

    ctx.complete       = 0;
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

static int client_do_work(ucp_worker_h ucp_worker, ucp_ep_h ep)
{
    int i, ret = 0;
    ucs_status_t status;

    connection_closed = 0;

    ret = client_send(ucp_worker, ep);
    //printf("%d\n",ret);
    ret = client_recv(ucp_worker, ep);
    //printf("%d\n",ret);
    printf("%s FIN message\n", "received");

    return ret;
}

int main(void)
{
    ucp_params_t ucp_params;
    ucp_worker_params_t worker_params;
    ucs_status_t status;
    ucp_ep_h     client_ep;
    char* server_addr="127.0.0.1";
    ucp_ep_params_t ep_params;
    struct sockaddr_storage connect_addr;
    //struct sockaddr_storage saddr;
    int ret = 0;

    memset(&ucp_params, 0, sizeof(ucp_params));

    /* UCP initialization */
    ucp_params.field_mask = UCP_PARAM_FIELD_FEATURES | UCP_PARAM_FIELD_NAME;
    ucp_params.name       = "client";

    ucp_params.features = UCP_FEATURE_STREAM;
    status = ucp_init(&ucp_params, NULL, &ucp_context); //init ucp_context
    if (status != UCS_OK) {
        fprintf(stderr, "failed to ucp_init (%s)\n", ucs_status_string(status));
        ret = -1;
        return -1;
    }

    //ret = init_worker(ucp_context, &ucp_worker);

    memset(&worker_params, 0, sizeof(worker_params));

    worker_params.field_mask  = UCP_WORKER_PARAM_FIELD_THREAD_MODE;
    worker_params.thread_mode = UCS_THREAD_MODE_SINGLE;

    status = ucp_worker_create(ucp_context, &worker_params, &ucp_worker); //create ucp_worker
    if (status != UCS_OK) {
        fprintf(stderr, "failed to ucp_worker_create (%s)\n", ucs_status_string(status));
        ret = -1;
    }

    if (ret != 0) {
        return -1;
    }
    memset(&connect_addr, 0, sizeof(connect_addr));
    struct sockaddr_in *sa_in;
    sa_in = (struct sockaddr_in*)&connect_addr;
    inet_pton(AF_INET, server_addr, &sa_in->sin_addr);\
    //printf("%d\n",sa_in->sin_addr.s_addr);
    sa_in->sin_family = AF_INET;
    sa_in->sin_port   = htons(server_port);

    ep_params.field_mask       = UCP_EP_PARAM_FIELD_FLAGS       |
                                 UCP_EP_PARAM_FIELD_SOCK_ADDR   |
                                 UCP_EP_PARAM_FIELD_ERR_HANDLER |
                                 UCP_EP_PARAM_FIELD_ERR_HANDLING_MODE;
    ep_params.err_mode         = UCP_ERR_HANDLING_MODE_PEER;
    ep_params.err_handler.cb   = err_cb;
    ep_params.err_handler.arg  = NULL;
    ep_params.flags            = UCP_EP_PARAMS_FLAGS_CLIENT_SERVER;
    ep_params.sockaddr.addr    = (struct sockaddr*)&connect_addr;
    ep_params.sockaddr.addrlen = sizeof(connect_addr);

    status = ucp_ep_create(ucp_worker, &ep_params, &client_ep); //create endpoint and connect to a remote
    //status = ucp_worker_progress(ucp_worker);
    if (status != UCS_OK) {
        fprintf(stderr, "failed to connect to %s (%s)\n", server_addr,
                ucs_status_string(status));
    }

    //ret = client_do_work(ucp_worker, client_ep);
    for(;;)
    {
        ucp_worker_progress(ucp_worker);
    }
    /* Close the endpoint to the server */
    //ucp_ep_close_nb(client_ep, UCP_EP_CLOSE_FLAG_FORCE);
    ep_close(ucp_worker, client_ep, UCP_EP_CLOSE_FLAG_FORCE);
    //for(;;);
    return ret;
}