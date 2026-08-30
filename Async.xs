#include "EXTERN.h"
#include "perl.h"
#include "XSUB.h"
#include "consumer_abi.h"

typedef struct lea_recv_ctx_s {
    const les_consumer_host_api_v1_t *host;
    void *host_context;
    SV *stream;
    SV *result;
    SV *failure;
    SV *on_ready;
    SV *on_cancel;
    int armed;
    int ready;
    int cancelled;
    int in_delivery;
} lea_recv_ctx_t;

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
    hv_store(hv, LEA_CTX_KEY, LEA_CTX_KEY_LEN,
        newSVuv(PTR2UV(ctx)), 0);
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

static void
lea_clear_sv(SV **slot)
{
    if (*slot) {
        SvREFCNT_dec(*slot);
        *slot = NULL;
    }
}

static void
lea_call(SV *code, SV *arg)
{
    dSP;
    SV *error = NULL;

    ENTER;
    SAVETMPS;
    PUSHMARK(SP);
    XPUSHs(arg);
    PUTBACK;
    call_sv(code, G_VOID | G_EVAL);
    SPAGAIN;
    if (SvTRUE(ERRSV))
        error = newSVsv(ERRSV);
    PUTBACK;
    FREETMPS;
    LEAVE;

    if (error)
        croak_sv(error);
}

static void
lea_finish_ready(pTHX_ lea_recv_ctx_t *ctx)
{
    SV *callback;
    if (!ctx->on_ready)
        return;
    callback = ctx->on_ready;
    ctx->on_ready = NULL;
    ctx->in_delivery = 1;
    lea_call(callback, ctx->stream);
    ctx->in_delivery = 0;
    SvREFCNT_dec(callback);
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
    ctx->stream = SvREFCNT_inc(stream);
    lea_store_ctx(stream, ctx);
    return ctx;
}

static int
lea_consumer_message(pTHX_ void *context, SV *message)
{
    lea_recv_ctx_t *ctx = (lea_recv_ctx_t *)context;

    if (!ctx->armed)
        return LES_CONSUMER_PAUSE;

    lea_clear_sv(&ctx->result);
    lea_clear_sv(&ctx->failure);
    ctx->result = SvREFCNT_inc(message);
    ctx->armed = 0;
    ctx->ready = 1;
    ctx->cancelled = 0;

    lea_finish_ready(aTHX_ ctx);
    return ctx->armed ? LES_CONSUMER_CONTINUE : LES_CONSUMER_PAUSE;
}

static void
lea_consumer_event(pTHX_ void *context, uint32_t event, int error,
    const char *message)
{
    lea_recv_ctx_t *ctx = (lea_recv_ctx_t *)context;
    SV *text;

    if (!ctx->armed || ctx->ready)
        return;

    ctx->armed = 0;
    ctx->ready = 1;
    ctx->cancelled = 0;
    lea_clear_sv(&ctx->result);
    lea_clear_sv(&ctx->failure);

    if (event != LES_CONSUMER_EVENT_EOF) {
        if (message && *message)
            text = newSVpv(message, 0);
        else if (error)
            text = newSVpvf("Linux::Event Stream consumer error %d", error);
        else
            text = newSVpv("Linux::Event Stream closed", 0);
        ctx->failure = text;
    }

    lea_finish_ready(aTHX_ ctx);
}

static void
lea_consumer_destroy(pTHX_ void *context)
{
    lea_recv_ctx_t *ctx = (lea_recv_ctx_t *)context;
    HV *hv;

    if (!ctx)
        return;
    if (ctx->stream && SvROK(ctx->stream) && SvTYPE(SvRV(ctx->stream)) == SVt_PVHV) {
        hv = (HV *)SvRV(ctx->stream);
        hv_delete(hv, LEA_CTX_KEY, LEA_CTX_KEY_LEN, G_DISCARD);
    }
    lea_clear_sv(&ctx->result);
    lea_clear_sv(&ctx->failure);
    lea_clear_sv(&ctx->on_ready);
    lea_clear_sv(&ctx->on_cancel);
    if (ctx->stream)
        SvREFCNT_dec(ctx->stream);
    Safefree(ctx);
}

static const les_consumer_ops_v1_t lea_consumer_ops = {
    LES_CONSUMER_ABI_VERSION,
    sizeof(les_consumer_ops_v1_t),
    "Linux::Event::Async::Stream",
    LES_CONSUMER_F_START_PAUSED,
    lea_consumer_create,
    lea_consumer_message,
    lea_consumer_event,
    lea_consumer_destroy
};

MODULE = Linux::Event::Async    PACKAGE = Linux::Event::Async::Stream

UV
_consumer_operations_address()
    CODE:
        RETVAL = PTR2UV(&lea_consumer_ops);
    OUTPUT:
        RETVAL

void
_recv_arm(stream)
    SV *stream
    PREINIT:
        lea_recv_ctx_t *ctx;
        int status;
    CODE:
        ctx = lea_get_ctx(stream);
        if (ctx->armed)
            croak("recv(): a receive is already pending");
        if (ctx->ready)
            croak("recv(): previous receive result has not been consumed");
        ctx->armed = 1;
        ctx->cancelled = 0;
        if (!ctx->in_delivery) {
            status = ctx->host->resume(aTHX_ ctx->host_context);
            if (status < 0) {
                ctx->armed = 0;
                croak("recv(): Linux::Event consumer resume failed");
            }
        }

int
_recv_is_ready(stream)
    SV *stream
    CODE:
        RETVAL = lea_get_ctx(stream)->ready;
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
        if (!ctx->ready)
            croak("AWAIT_GET called while receive is pending");
        failure = ctx->failure ? newSVsv(ctx->failure) : NULL;
        result = ctx->result ? newSVsv(ctx->result) : newSV(0);
        lea_clear_sv(&ctx->result);
        lea_clear_sv(&ctx->failure);
        ctx->ready = 0;
        ctx->cancelled = 0;
        if (failure)
            croak_sv(failure);
        RETVAL = result;
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
        if (ctx->ready) {
            lea_call(code, stream);
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
            lea_call(code, stream);
        }
        else {
            lea_clear_sv(&ctx->on_cancel);
            ctx->on_cancel = SvREFCNT_inc(code);
        }

void
_recv_cancel(stream)
    SV *stream
    PREINIT:
        lea_recv_ctx_t *ctx;
        SV *callback;
    CODE:
        ctx = lea_get_ctx(stream);
        if (!ctx->armed)
            XSRETURN_EMPTY;
        ctx->armed = 0;
        ctx->cancelled = 1;
        ctx->host->pause(aTHX_ ctx->host_context);
        callback = ctx->on_cancel;
        ctx->on_cancel = NULL;
        if (callback) {
            lea_call(callback, stream);
            SvREFCNT_dec(callback);
        }
