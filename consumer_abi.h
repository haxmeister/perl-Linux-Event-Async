#ifndef LINUX_EVENT_ASYNC_CONSUMER_ABI_H
#define LINUX_EVENT_ASYNC_CONSUMER_ABI_H

#include "EXTERN.h"
#include "perl.h"
#include <stddef.h>
#include <stdint.h>

#define LES_CONSUMER_ABI_VERSION 1U
#define LES_CONSUMER_F_START_PAUSED 0x01U
#define LES_CONSUMER_CONTINUE 0
#define LES_CONSUMER_PAUSE    1
#define LES_CONSUMER_CLOSE    2
#define LES_CONSUMER_ERROR    3
#define LES_CONSUMER_EVENT_EOF           1U
#define LES_CONSUMER_EVENT_READ_ERROR    2U
#define LES_CONSUMER_EVENT_FRAMING_ERROR 3U
#define LES_CONSUMER_EVENT_CLOSED        4U
#define LES_CONSUMER_EVENT_DETACHED      5U

typedef struct les_consumer_host_api_v1_s {
    uint32_t abi_version;
    size_t struct_size;
    int (*resume)(pTHX_ void *host_context);
    int (*pause)(pTHX_ void *host_context);
    SV *(*stream)(pTHX_ void *host_context);
    int (*is_closed)(pTHX_ void *host_context);
} les_consumer_host_api_v1_t;

typedef struct les_consumer_ops_v1_s {
    uint32_t abi_version;
    size_t struct_size;
    const char *name;
    uint32_t flags;
    void *(*create)(pTHX_ const les_consumer_host_api_v1_t *host,
        void *host_context, SV *stream);
    int (*message)(pTHX_ void *context, SV *message);
    void (*event)(pTHX_ void *context, uint32_t event, int error,
        const char *message);
    void (*destroy)(pTHX_ void *context);
} les_consumer_ops_v1_t;

#endif
