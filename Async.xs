#include "EXTERN.h"
#include "perl.h"
#include "XSUB.h"
#include "consumer_abi.h"

#define LEA_PREFETCH_MAX_MESSAGES 64U
#define LEA_PREFETCH_MAX_BYTES 262144U

typedef struct lea_recv_ctx_s {
    const les_consumer_host_api_v1_t *host;
    void *host_context;
    SV *awaitable;
    SV *result;
    SV *failure;
    SV *terminal_failure;
    SV *on_ready;
    SV *on_cancel;
    SV *prefetch[LEA_PREFETCH_MAX_MESSAGES];
    UV prefetch_bytes;
    unsigned int prefetch_head;
    unsigned int prefetch_count;
    int armed;
    int ready;
    int cancelled;
    int in_delivery;
    int terminal;
    int flush_pending;
    int profile;
    unsigned long long profile_recv_calls;
    unsigned long long profile_recv_immediate;
    unsigned long long profile_messages;
    unsigned long long profile_ready_callbacks;
    unsigned long long profile_await_is_ready;
    unsigned long long profile_await_get;
    unsigned long long profile_await_on_ready;
    unsigned long long profile_await_suspended;
    unsigned long long profile_prefetched;
    unsigned long long profile_flushes;
} lea_recv_ctx_t;

#define LEA_PROFILE(ctx, field) \
    STMT_START { if ((ctx)->profile) (ctx)->field++; } STMT_END

#define LEA_CTX_KEY "_linux_event_async_recv_ctx"
#define LEA_CTX_KEY_LEN (sizeof(LEA_CTX_KEY) - 1)

static HV *
lea_stream_hv(SV *stream)
{
    if (!SvROK(stream) || SvTYPE(SvRV(stream)) != SVt_PVHV)
        croak("Linux::Event::Async::Stream requires a hash-based Stream object");
    return (HV *)SvRV(stream);
}

static void
lea_store_ctx(SV *stream, lea_recv_ctx_t *ctx)
{
    HV *hv = lea_stream_hv(stream);
    hv_store(hv, LEA_CTX_KEY, LEA_CTX_KEY_LEN, newSVuv(PTR2UV(ctx)), 0);
}

static lea_recv_ctx_t *
lea_get_ctx(SV *stream)
{
    HV *hv = lea_stream_hv(stream);
    SV **svp = hv_fetch(hv, LEA_CTX_KEY, LEA_CTX_KEY_LEN, 0);
    if (!svp || !SvOK(*svp))
        croak("Linux::Event::Async receive context is not available");
    return INT2PTR(lea_recv_ctx_t *, SvUV(*svp));
}

static lea_recv_ctx_t *
lea_awaitable_ctx(SV *awaitable)
{
    lea_recv_ctx_t *ctx;

    if (!SvROK(awaitable) || !SvIOK(SvRV(awaitable)))
        croak("not a Linux::Event::Async Stream Awaitable");
    ctx = INT2PTR(lea_recv_ctx_t *, SvUV(SvRV(awaitable)));
    if (!ctx)
        croak("Linux::Event::Async Stream Awaitable is detached");
    return ctx;
}

static SV *
lea_awaitable_new(lea_recv_ctx_t *ctx)
{
    return sv_bless(newRV_noinc(newSVuv(PTR2UV(ctx))),
        gv_stashpv("Linux::Event::Async::Stream::Awaitable", GV_ADD));
}

static void
lea_clear_sv(SV **slot)
{
    if (*slot) {
        SvREFCNT_dec(*slot);
        *slot = NULL;
    }
}

static void
lea_prefetch_push(lea_recv_ctx_t *ctx, SV *message)
{
    unsigned int tail;
    UV bytes = (UV)SvCUR(message);

    if (ctx->prefetch_count >= LEA_PREFETCH_MAX_MESSAGES)
        croak("Linux::Event::Async receive prefetch ring overflow");
    tail = (ctx->prefetch_head + ctx->prefetch_count)
        % LEA_PREFETCH_MAX_MESSAGES;
    ctx->prefetch[tail] = SvREFCNT_inc(message);
    ctx->prefetch_count++;
    if (bytes > (UV)-1 - ctx->prefetch_bytes)
        ctx->prefetch_bytes = (UV)-1;
    else
        ctx->prefetch_bytes += bytes;
    LEA_PROFILE(ctx, profile_prefetched);
}

static SV *
lea_prefetch_shift(lea_recv_ctx_t *ctx)
{
    SV *message;
    UV bytes;

    if (!ctx->prefetch_count)
        return NULL;
    message = ctx->prefetch[ctx->prefetch_head];
    ctx->prefetch[ctx->prefetch_head] = NULL;
    ctx->prefetch_head = (ctx->prefetch_head + 1)
        % LEA_PREFETCH_MAX_MESSAGES;
    ctx->prefetch_count--;
    bytes = (UV)SvCUR(message);
    ctx->prefetch_bytes = bytes > ctx->prefetch_bytes
        ? 0 : ctx->prefetch_bytes - bytes;
    if (!ctx->prefetch_count)
        ctx->prefetch_head = 0;
    return message;
}

static SV *
lea_error_new(uint32_t event, int error, const char *message)
{
    HV *hv = newHV();
    const char *type = "event";
    const char *operation = "read";
    const char *text = message && *message ? message : "Linux::Event Stream failure";

    if (event == LES_CONSUMER_EVENT_READ_ERROR)
        type = "io";
    else if (event == LES_CONSUMER_EVENT_FRAMING_ERROR)
        type = "framing";
    else if (event == LES_CONSUMER_EVENT_CLOSED)
        text = message && *message ? message : "Stream closed while receive was pending";
    else if (event == LES_CONSUMER_EVENT_DETACHED)
        text = message && *message ? message : "Stream detached while receive was pending";

    hv_stores(hv, "type", newSVpv(type, 0));
    hv_stores(hv, "operation", newSVpv(operation, 0));
    hv_stores(hv, "message", newSVpv(text, 0));
    if (error)
        hv_stores(hv, "errno", newSViv(error));

    return sv_bless(newRV_noinc((SV *)hv), gv_stashpv("Linux::Event::Error", GV_ADD));
}

static void
lea_call_owned(pTHX_ lea_recv_ctx_t *ctx, SV *code)
{
    dSP;

    ENTER;
    SAVETMPS;
    SAVEFREESV(code);
    SAVEINT(ctx->in_delivery);
    ctx->in_delivery = 1;
    PUSHMARK(SP);
    PUTBACK;
    call_sv(code, G_DISCARD | G_VOID);
    FREETMPS;
    LEAVE;
}

static void
lea_fire_ready(pTHX_ lea_recv_ctx_t *ctx)
{
    SV *callback = ctx->on_ready;
    ctx->on_ready = NULL;
    if (callback) {
        LEA_PROFILE(ctx, profile_ready_callbacks);
        lea_call_owned(aTHX_ ctx, callback);
    }
}

static void
lea_fire_cancel(pTHX_ lea_recv_ctx_t *ctx)
{
    SV *callback = ctx->on_cancel;
    ctx->on_cancel = NULL;
    if (callback)
        lea_call_owned(aTHX_ ctx, callback);
}

static void
lea_prepare_terminal_result(lea_recv_ctx_t *ctx)
{
    lea_clear_sv(&ctx->result);
    lea_clear_sv(&ctx->failure);
    if (ctx->terminal_failure)
        ctx->failure = SvREFCNT_inc(ctx->terminal_failure);
    ctx->ready = 1;
    ctx->cancelled = 0;
}

static void *
lea_consumer_create(pTHX_ const les_consumer_host_api_v1_t *host,
    void *host_context, SV *stream)
{
    lea_recv_ctx_t *ctx;

    Newxz(ctx, 1, lea_recv_ctx_t);
    if (!ctx)
        return NULL;
    ctx->host = host;
    ctx->host_context = host_context;
    ctx->awaitable = lea_awaitable_new(ctx);
    lea_store_ctx(stream, ctx);
    return ctx;
}

static int
lea_consumer_message(pTHX_ void *context, SV *message)
{
    lea_recv_ctx_t *ctx = (lea_recv_ctx_t *)context;

    LEA_PROFILE(ctx, profile_messages);

    if (!ctx->armed) {
        lea_prefetch_push(ctx, message);
        if (ctx->prefetch_count >= LEA_PREFETCH_MAX_MESSAGES
            || ctx->prefetch_bytes >= LEA_PREFETCH_MAX_BYTES) {
            ctx->flush_pending = 0;
            LEA_PROFILE(ctx, profile_flushes);
            lea_fire_ready(aTHX_ ctx);
            return ctx->armed ? LES_CONSUMER_CONTINUE : LES_CONSUMER_PAUSE;
        }
        return LES_CONSUMER_CONTINUE;
    }

    lea_clear_sv(&ctx->result);
    lea_clear_sv(&ctx->failure);
    ctx->result = SvREFCNT_inc(message);
    ctx->armed = 0;
    ctx->ready = 1;
    ctx->cancelled = 0;
    ctx->flush_pending = 1;

    return LES_CONSUMER_CONTINUE;
}

static int
lea_consumer_flush(pTHX_ void *context)
{
    lea_recv_ctx_t *ctx = (lea_recv_ctx_t *)context;

    if (ctx->flush_pending) {
        ctx->flush_pending = 0;
        LEA_PROFILE(ctx, profile_flushes);
        lea_fire_ready(aTHX_ ctx);
    }
    return ctx->armed ? LES_CONSUMER_CONTINUE : LES_CONSUMER_PAUSE;
}

static void
lea_consumer_event(pTHX_ void *context, uint32_t event, int error,
    const char *message)
{
    lea_recv_ctx_t *ctx = (lea_recv_ctx_t *)context;

    if (ctx->terminal)
        return;

    ctx->terminal = 1;
    lea_clear_sv(&ctx->terminal_failure);
    if (event != LES_CONSUMER_EVENT_EOF)
        ctx->terminal_failure = lea_error_new(event, error, message);

    if (!ctx->armed)
        return;

    ctx->armed = 0;
    lea_prepare_terminal_result(ctx);
    lea_fire_ready(aTHX_ ctx);
}

static void
lea_consumer_destroy(pTHX_ void *context)
{
    lea_recv_ctx_t *ctx = (lea_recv_ctx_t *)context;
    SV *stream;

    if (!ctx)
        return;

    stream = ctx->host->stream(aTHX_ ctx->host_context);
    if (stream && SvOK(stream) && SvROK(stream)
        && SvTYPE(SvRV(stream)) == SVt_PVHV) {
        hv_delete((HV *)SvRV(stream), LEA_CTX_KEY, LEA_CTX_KEY_LEN, G_DISCARD);
    }

    lea_clear_sv(&ctx->result);
    lea_clear_sv(&ctx->failure);
    lea_clear_sv(&ctx->terminal_failure);
    lea_clear_sv(&ctx->on_ready);
    lea_clear_sv(&ctx->on_cancel);
    while (ctx->prefetch_count) {
        SV *message = lea_prefetch_shift(ctx);
        SvREFCNT_dec(message);
    }
    if (ctx->awaitable) {
        sv_setuv(SvRV(ctx->awaitable), 0);
        SvREFCNT_dec(ctx->awaitable);
        ctx->awaitable = NULL;
    }
    Safefree(ctx);
}

static const les_consumer_ops_v1_t lea_consumer_ops = {
    LES_CONSUMER_ABI_VERSION,
    sizeof(les_consumer_ops_v1_t),
    "Linux::Event::Async::Stream",
    LES_CONSUMER_F_START_PAUSED | LES_CONSUMER_F_WANT_FLUSH,
    lea_consumer_create,
    lea_consumer_message,
    lea_consumer_event,
    lea_consumer_destroy,
    lea_consumer_flush
};

static lea_recv_ctx_t *
lea_recv_arm(pTHX_ SV *stream)
{
    lea_recv_ctx_t *ctx = lea_get_ctx(stream);
    int status;

    LEA_PROFILE(ctx, profile_recv_calls);

    if (ctx->armed)
        croak("recv(): a receive is already pending");
    if (ctx->ready)
        croak("recv(): previous receive result has not been consumed");

    ctx->cancelled = 0;
    lea_clear_sv(&ctx->on_ready);
    lea_clear_sv(&ctx->on_cancel);

    if (ctx->prefetch_count) {
        ctx->result = lea_prefetch_shift(ctx);
        ctx->ready = 1;
        LEA_PROFILE(ctx, profile_recv_immediate);
        return ctx;
    }

    if (ctx->terminal) {
        lea_prepare_terminal_result(ctx);
        return ctx;
    }

    ctx->armed = 1;
    if (!ctx->in_delivery) {
        status = ctx->host->resume(aTHX_ ctx->host_context);
        if (status < 0) {
            ctx->armed = 0;
            croak("recv(): Linux::Event consumer resume failed");
        }
        if (status == 0 && !ctx->ready && !ctx->terminal
            && ctx->host->is_closed(aTHX_ ctx->host_context)) {
            ctx->armed = 0;
            ctx->terminal = 1;
            ctx->terminal_failure = lea_error_new(
                LES_CONSUMER_EVENT_CLOSED, 0,
                "Stream closed while receive was being armed");
            lea_prepare_terminal_result(ctx);
        }
    }
    if (ctx->ready)
        LEA_PROFILE(ctx, profile_recv_immediate);
    return ctx;
}

MODULE = Linux::Event::Async    PACKAGE = Linux::Event::Async::Stream
PROTOTYPES: DISABLE

UV
_consumer_operations_address()
    CODE:
        RETVAL = PTR2UV(&lea_consumer_ops);
    OUTPUT:
        RETVAL

void
_recv_profile_start(stream)
    SV *stream
    PREINIT:
        lea_recv_ctx_t *ctx;
    CODE:
        ctx = lea_get_ctx(stream);
        ctx->profile = 1;
        ctx->profile_recv_calls = 0;
        ctx->profile_recv_immediate = 0;
        ctx->profile_messages = 0;
        ctx->profile_ready_callbacks = 0;
        ctx->profile_await_is_ready = 0;
        ctx->profile_await_get = 0;
        ctx->profile_await_on_ready = 0;
        ctx->profile_await_suspended = 0;
        ctx->profile_prefetched = 0;
        ctx->profile_flushes = 0;

SV *
_recv_profile_stats(stream)
    SV *stream
    PREINIT:
        lea_recv_ctx_t *ctx;
        HV *hv;
    CODE:
        ctx = lea_get_ctx(stream);
        hv = newHV();
        hv_stores(hv, "recv_calls", newSVuv(ctx->profile_recv_calls));
        hv_stores(hv, "recv_immediate", newSVuv(ctx->profile_recv_immediate));
        hv_stores(hv, "messages", newSVuv(ctx->profile_messages));
        hv_stores(hv, "ready_callbacks", newSVuv(ctx->profile_ready_callbacks));
        hv_stores(hv, "await_is_ready", newSVuv(ctx->profile_await_is_ready));
        hv_stores(hv, "await_get", newSVuv(ctx->profile_await_get));
        hv_stores(hv, "await_on_ready", newSVuv(ctx->profile_await_on_ready));
        hv_stores(hv, "await_suspended", newSVuv(ctx->profile_await_suspended));
        hv_stores(hv, "prefetched", newSVuv(ctx->profile_prefetched));
        hv_stores(hv, "flushes", newSVuv(ctx->profile_flushes));
        RETVAL = newRV_noinc((SV *)hv);
    OUTPUT:
        RETVAL

SV *
recv(stream)
    SV *stream
    PREINIT:
        lea_recv_ctx_t *ctx;
    CODE:
        ctx = lea_recv_arm(aTHX_ stream);
        RETVAL = SvREFCNT_inc(ctx->awaitable);
    OUTPUT:
        RETVAL

SV *
_recv_arm(stream)
    SV *stream
    PREINIT:
        lea_recv_ctx_t *ctx;
    CODE:
        ctx = lea_recv_arm(aTHX_ stream);
        RETVAL = SvREFCNT_inc(ctx->awaitable);
    OUTPUT:
        RETVAL

int
_recv_is_ready(stream)
    SV *stream
    PREINIT:
        lea_recv_ctx_t *ctx;
    CODE:
        ctx = lea_get_ctx(stream);
        RETVAL = ctx->ready || ctx->cancelled;
    OUTPUT:
        RETVAL

int
_recv_is_cancelled(stream)
    SV *stream
    CODE:
        RETVAL = lea_get_ctx(stream)->cancelled;
    OUTPUT:
        RETVAL

SV *
_recv_get(stream)
    SV *stream
    PREINIT:
        lea_recv_ctx_t *ctx;
        SV *result;
        SV *failure;
    CODE:
        ctx = lea_get_ctx(stream);
        if (ctx->cancelled)
            croak("cannot get a cancelled receive");
        if (!ctx->ready)
            croak("AWAIT_GET called while receive is pending");

        failure = ctx->failure;
        ctx->failure = NULL;
        result = ctx->result;
        ctx->result = NULL;
        ctx->ready = 0;

        if (failure)
            croak_sv(failure);
        RETVAL = result ? result : newSV(0);
    OUTPUT:
        RETVAL

void
_recv_on_ready(stream, code)
    SV *stream
    SV *code
    PREINIT:
        lea_recv_ctx_t *ctx;
    CODE:
        ctx = lea_get_ctx(stream);
        if (!SvROK(code) || SvTYPE(SvRV(code)) != SVt_PVCV)
            croak("AWAIT_ON_READY requires a coderef");
        if (ctx->ready || ctx->cancelled) {
            lea_call_owned(aTHX_ ctx, SvREFCNT_inc(code));
        }
        else {
            lea_clear_sv(&ctx->on_ready);
            ctx->on_ready = SvREFCNT_inc(code);
        }

void
_recv_on_cancel(stream, code)
    SV *stream
    SV *code
    PREINIT:
        lea_recv_ctx_t *ctx;
    CODE:
        ctx = lea_get_ctx(stream);
        if (!SvROK(code) || SvTYPE(SvRV(code)) != SVt_PVCV)
            croak("AWAIT_ON_CANCEL requires a coderef");
        if (ctx->cancelled) {
            lea_call_owned(aTHX_ ctx, SvREFCNT_inc(code));
        }
        else if (ctx->armed) {
            lea_clear_sv(&ctx->on_cancel);
            ctx->on_cancel = SvREFCNT_inc(code);
        }

void
_recv_cancel(stream)
    SV *stream
    PREINIT:
        lea_recv_ctx_t *ctx;
    CODE:
        ctx = lea_get_ctx(stream);
        if (!ctx->armed)
            XSRETURN_EMPTY;

        ctx->armed = 0;
        ctx->cancelled = 1;
        if (!ctx->terminal)
            ctx->host->pause(aTHX_ ctx->host_context);
        lea_fire_cancel(aTHX_ ctx);
        lea_fire_ready(aTHX_ ctx);

MODULE = Linux::Event::Async    PACKAGE = Linux::Event::Async::Stream::Awaitable
PROTOTYPES: DISABLE

int
AWAIT_IS_READY(awaitable)
    SV *awaitable
    PREINIT:
        lea_recv_ctx_t *ctx;
    CODE:
        ctx = lea_awaitable_ctx(awaitable);
        LEA_PROFILE(ctx, profile_await_is_ready);
        RETVAL = ctx->ready || ctx->cancelled;
    OUTPUT:
        RETVAL

int
AWAIT_IS_CANCELLED(awaitable)
    SV *awaitable
    CODE:
        RETVAL = lea_awaitable_ctx(awaitable)->cancelled;
    OUTPUT:
        RETVAL

SV *
AWAIT_GET(awaitable)
    SV *awaitable
    PREINIT:
        lea_recv_ctx_t *ctx;
        SV *result;
        SV *failure;
    CODE:
        ctx = lea_awaitable_ctx(awaitable);
        LEA_PROFILE(ctx, profile_await_get);
        if (ctx->cancelled)
            croak("cannot get a cancelled receive");
        if (!ctx->ready)
            croak("AWAIT_GET called while receive is pending");

        failure = ctx->failure;
        ctx->failure = NULL;
        result = ctx->result;
        ctx->result = NULL;
        ctx->ready = 0;

        if (failure)
            croak_sv(failure);
        RETVAL = result ? result : newSV(0);
    OUTPUT:
        RETVAL

void
AWAIT_ON_READY(awaitable, code)
    SV *awaitable
    SV *code
    PREINIT:
        lea_recv_ctx_t *ctx;
    CODE:
        ctx = lea_awaitable_ctx(awaitable);
        LEA_PROFILE(ctx, profile_await_on_ready);
        if (!SvROK(code) || SvTYPE(SvRV(code)) != SVt_PVCV)
            croak("AWAIT_ON_READY requires a coderef");
        if (ctx->ready || ctx->cancelled) {
            lea_call_owned(aTHX_ ctx, SvREFCNT_inc(code));
        }
        else {
            LEA_PROFILE(ctx, profile_await_suspended);
            lea_clear_sv(&ctx->on_ready);
            ctx->on_ready = SvREFCNT_inc(code);
        }

void
AWAIT_ON_CANCEL(awaitable, code)
    SV *awaitable
    SV *code
    PREINIT:
        lea_recv_ctx_t *ctx;
    CODE:
        ctx = lea_awaitable_ctx(awaitable);
        if (!SvROK(code) || SvTYPE(SvRV(code)) != SVt_PVCV)
            croak("AWAIT_ON_CANCEL requires a coderef");
        if (ctx->cancelled) {
            lea_call_owned(aTHX_ ctx, SvREFCNT_inc(code));
        }
        else if (ctx->armed) {
            lea_clear_sv(&ctx->on_cancel);
            ctx->on_cancel = SvREFCNT_inc(code);
        }

void
cancel_recv(awaitable)
    SV *awaitable
    PREINIT:
        lea_recv_ctx_t *ctx;
    CODE:
        ctx = lea_awaitable_ctx(awaitable);
        if (!ctx->armed)
            XSRETURN_EMPTY;

        ctx->armed = 0;
        ctx->cancelled = 1;
        if (!ctx->terminal)
            ctx->host->pause(aTHX_ ctx->host_context);
        lea_fire_cancel(aTHX_ ctx);
        lea_fire_ready(aTHX_ ctx);

SV *
_stream(awaitable)
    SV *awaitable
    PREINIT:
        lea_recv_ctx_t *ctx;
        SV *stream;
    CODE:
        ctx = lea_awaitable_ctx(awaitable);
        stream = ctx->host->stream(aTHX_ ctx->host_context);
        if (!stream || !SvOK(stream))
            croak("Linux::Event::Async Stream host is no longer available");
        RETVAL = newSVsv(stream);
    OUTPUT:
        RETVAL
