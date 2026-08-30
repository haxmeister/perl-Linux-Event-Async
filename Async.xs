#include "EXTERN.h"
#include "perl.h"
#include "XSUB.h"
#include "consumer_abi.h"

/* Native Future state used as Future::AsyncAwait's coroutine completion class. */
enum {
    LEAF_PENDING = 0,
    LEAF_DONE = 1,
    LEAF_FAILED = 2,
    LEAF_CANCELLED = 3
};

typedef struct leaf_future_s {
    int state;
    SV *loop_sv;
    SV *result;
    AV *results;
    SV *failure;
    SV *ready_callback;
    AV *ready_callbacks;
    AV *cancel_callbacks;
    AV *cancel_chain;
} leaf_future_t;

static leaf_future_t *
leaf_from_sv(SV *obj)
{
    leaf_future_t *future;

    if (!sv_isobject(obj) || !SvROK(obj))
        croak("not a Linux::Event::Async::Future object");
    future = INT2PTR(leaf_future_t *, SvIV((SV *)SvRV(obj)));
    if (!future)
        croak("Linux::Event::Async::Future object is destroyed");
    return future;
}

static SV *
leaf_new(const char *class_name, SV *loop_sv)
{
    leaf_future_t *future;

    Newxz(future, 1, leaf_future_t);
    future->state = LEAF_PENDING;
    if (loop_sv && SvOK(loop_sv))
        future->loop_sv = newSVsv(loop_sv);
    return sv_bless(newRV_noinc(newSViv(PTR2IV(future))),
        gv_stashpv(class_name, GV_ADD));
}

static void
leaf_require_pending(leaf_future_t *future)
{
    if (future->state != LEAF_PENDING)
        croak("future is not pending");
}

static void
leaf_require_callback(SV *callback)
{
    if (!callback || !SvROK(callback)
        || SvTYPE(SvRV(callback)) != SVt_PVCV)
        croak("Future callback must be a coderef");
}

static void
leaf_call_callback(pTHX_ SV *callback)
{
    dSP;

    ENTER;
    SAVETMPS;
    PUSHMARK(SP);
    PUTBACK;
    call_sv(callback, G_DISCARD | G_VOID);
    FREETMPS;
    LEAVE;
}

static void
leaf_add_callback(SV **single, AV **overflow, SV *callback)
{
    if (!*single) {
        *single = newSVsv(callback);
        return;
    }
    if (!*overflow)
        *overflow = newAV();
    av_push(*overflow, newSVsv(callback));
}

static void
leaf_call_callback_catching(pTHX_ SV *callback, SV **failure)
{
    dSP;

    if (!callback)
        return;
    ENTER;
    SAVETMPS;
    PUSHMARK(SP);
    PUTBACK;
    call_sv(callback, G_DISCARD | G_VOID | G_EVAL);
    if (SvTRUE(ERRSV)) {
        if (!*failure)
            *failure = newSVsv(ERRSV);
        sv_setsv(ERRSV, &PL_sv_undef);
    }
    FREETMPS;
    LEAVE;
    SvREFCNT_dec(callback);
}

static void
leaf_call_callbacks(pTHX_ AV *callbacks, SV **failure)
{
    SSize_t index;
    SSize_t count;

    if (!callbacks)
        return;
    count = av_count(callbacks);
    for (index = 0; index < count; index++) {
        SV **callback = av_fetch(callbacks, index, 0);
        if (callback && *callback) {
            dSP;
            ENTER;
            SAVETMPS;
            PUSHMARK(SP);
            PUTBACK;
            call_sv(*callback, G_DISCARD | G_VOID | G_EVAL);
            if (SvTRUE(ERRSV)) {
                if (!*failure)
                    *failure = newSVsv(ERRSV);
                sv_setsv(ERRSV, &PL_sv_undef);
            }
            FREETMPS;
            LEAVE;
        }
    }
    SvREFCNT_dec((SV *)callbacks);
}

static void
leaf_cancel_chain(pTHX_ AV *chain, SV **failure)
{
    SSize_t index;
    SSize_t count;

    if (!chain)
        return;
    count = av_count(chain);
    for (index = 0; index < count; index++) {
        SV **target = av_fetch(chain, index, 0);
        dSP;
        if (!target || !*target)
            continue;
        ENTER;
        SAVETMPS;
        PUSHMARK(SP);
        PUSHs(*target);
        PUTBACK;
        call_method("cancel", G_DISCARD | G_VOID | G_EVAL);
        if (SvTRUE(ERRSV)) {
            if (!*failure)
                *failure = newSVsv(ERRSV);
            sv_setsv(ERRSV, &PL_sv_undef);
        }
        FREETMPS;
        LEAVE;
    }
    SvREFCNT_dec((SV *)chain);
}

static void
leaf_discard_cancel_state(leaf_future_t *future)
{
    if (future->cancel_callbacks) {
        SvREFCNT_dec((SV *)future->cancel_callbacks);
        future->cancel_callbacks = NULL;
    }
    if (future->cancel_chain) {
        SvREFCNT_dec((SV *)future->cancel_chain);
        future->cancel_chain = NULL;
    }
}

static void
leaf_notify_ready(pTHX_ leaf_future_t *future)
{
    SV *callback = future->ready_callback;
    AV *callbacks = future->ready_callbacks;
    SV *failure = NULL;

    future->ready_callback = NULL;
    future->ready_callbacks = NULL;
    leaf_call_callback_catching(aTHX_ callback, &failure);
    leaf_call_callbacks(aTHX_ callbacks, &failure);
    if (failure)
        croak_sv(sv_2mortal(failure));
}

static void
leaf_set_done(pTHX_ leaf_future_t *future, I32 first, I32 last, SV **stack)
{
    I32 count;
    I32 index;

    leaf_require_pending(future);
    count = last >= first ? last - first + 1 : 0;
    if (count == 1) {
        future->result = newSVsv(stack[first]);
    } else if (count > 1) {
        future->results = newAV();
        for (index = first; index <= last; index++)
            av_push(future->results, newSVsv(stack[index]));
    }
    future->state = LEAF_DONE;
    leaf_discard_cancel_state(future);
    leaf_notify_ready(aTHX_ future);
}

static void
leaf_set_failed(pTHX_ leaf_future_t *future, SV *failure)
{
    leaf_require_pending(future);
    future->failure = newSVsv(failure);
    future->state = LEAF_FAILED;
    leaf_discard_cancel_state(future);
    leaf_notify_ready(aTHX_ future);
}

MODULE = Linux::Event::Async    PACKAGE = Linux::Event::Async::Future
PROTOTYPES: DISABLE

SV *
new(CLASS, ...)
    const char *CLASS
    PREINIT:
        SV *loop = &PL_sv_undef;
        const char *key;
    CODE:
        if (items == 2) {
            loop = ST(1);
        } else if (items == 3) {
            key = SvPV_nolen(ST(1));
            if (strNE(key, "loop"))
                croak("unknown Future option: %s", key);
            loop = ST(2);
        } else if (items != 1) {
            croak("Future->new accepts only an optional loop");
        }
        if (loop && SvOK(loop)
            && (!sv_isobject(loop)
                || !sv_derived_from(loop, "Linux::Event::Loop")))
            croak("Future loop must be a Linux::Event::Loop object");
        RETVAL = leaf_new(CLASS, loop);
    OUTPUT:
        RETVAL

SV *
AWAIT_NEW_DONE(CLASS, ...)
    const char *CLASS
    PREINIT:
        leaf_future_t *future;
    CODE:
        RETVAL = leaf_new(CLASS, NULL);
        future = leaf_from_sv(RETVAL);
        leaf_set_done(aTHX_ future, 1, items - 1, &ST(0));
    OUTPUT:
        RETVAL

SV *
AWAIT_NEW_FAIL(CLASS, failure)
    const char *CLASS
    SV *failure
    PREINIT:
        leaf_future_t *future;
    CODE:
        RETVAL = leaf_new(CLASS, NULL);
        future = leaf_from_sv(RETVAL);
        leaf_set_failed(aTHX_ future, failure);
    OUTPUT:
        RETVAL

SV *
AWAIT_CLONE(future_obj)
    SV *future_obj
    PREINIT:
        leaf_future_t *future;
        const char *class_name;
    CODE:
        future = leaf_from_sv(future_obj);
        leaf_require_pending(future);
        class_name = HvNAME(SvSTASH(SvRV(future_obj)));
        RETVAL = leaf_new(class_name, future->loop_sv);
    OUTPUT:
        RETVAL

SV *
AWAIT_DONE(future_obj, ...)
    SV *future_obj
    PREINIT:
        leaf_future_t *future;
    CODE:
        future = leaf_from_sv(future_obj);
        leaf_set_done(aTHX_ future, 1, items - 1, &ST(0));
        RETVAL = newSVsv(future_obj);
    OUTPUT:
        RETVAL

SV *
AWAIT_FAIL(future_obj, failure)
    SV *future_obj
    SV *failure
    PREINIT:
        leaf_future_t *future;
    CODE:
        future = leaf_from_sv(future_obj);
        leaf_set_failed(aTHX_ future, failure);
        RETVAL = newSVsv(future_obj);
    OUTPUT:
        RETVAL

int
AWAIT_IS_READY(future_obj)
    SV *future_obj
    CODE:
        RETVAL = leaf_from_sv(future_obj)->state != LEAF_PENDING;
    OUTPUT:
        RETVAL

int
AWAIT_IS_CANCELLED(future_obj)
    SV *future_obj
    CODE:
        RETVAL = leaf_from_sv(future_obj)->state == LEAF_CANCELLED;
    OUTPUT:
        RETVAL

void
AWAIT_GET(future_obj)
    SV *future_obj
    PREINIT:
        leaf_future_t *future;
        SSize_t count;
        SSize_t index;
        SV **value;
    PPCODE:
        future = leaf_from_sv(future_obj);
        if (future->state == LEAF_PENDING)
            croak("cannot get a pending future");
        if (future->state == LEAF_CANCELLED)
            croak("cannot get a cancelled future");
        if (future->state == LEAF_FAILED)
            croak_sv(future->failure);

        count = future->result ? 1
            : (future->results ? av_count(future->results) : 0);
        if (GIMME_V == G_VOID)
            XSRETURN_EMPTY;
        if (GIMME_V == G_SCALAR) {
            if (future->result)
                PUSHs(sv_2mortal(newSVsv(future->result)));
            else {
                value = count ? av_fetch(future->results, 0, 0) : NULL;
                PUSHs(value && *value
                    ? sv_2mortal(newSVsv(*value)) : &PL_sv_undef);
            }
            XSRETURN(1);
        }
        EXTEND(SP, count);
        if (future->result) {
            PUSHs(sv_2mortal(newSVsv(future->result)));
            XSRETURN(1);
        }
        for (index = 0; index < count; index++) {
            value = av_fetch(future->results, index, 0);
            PUSHs(value && *value
                ? sv_2mortal(newSVsv(*value)) : &PL_sv_undef);
        }

void
AWAIT_ON_READY(future_obj, callback)
    SV *future_obj
    SV *callback
    PREINIT:
        leaf_future_t *future;
    CODE:
        leaf_require_callback(callback);
        future = leaf_from_sv(future_obj);
        if (future->state != LEAF_PENDING)
            leaf_call_callback(aTHX_ callback);
        else
            leaf_add_callback(&future->ready_callback,
                &future->ready_callbacks, callback);

void
AWAIT_ON_CANCEL(future_obj, callback)
    SV *future_obj
    SV *callback
    PREINIT:
        leaf_future_t *future;
    CODE:
        leaf_require_callback(callback);
        future = leaf_from_sv(future_obj);
        if (future->state == LEAF_CANCELLED) {
            leaf_call_callback(aTHX_ callback);
        } else if (future->state == LEAF_PENDING) {
            if (!future->cancel_callbacks)
                future->cancel_callbacks = newAV();
            av_push(future->cancel_callbacks, newSVsv(callback));
        }

void
AWAIT_CHAIN_CANCEL(future_obj, target)
    SV *future_obj
    SV *target
    PREINIT:
        leaf_future_t *future;
    CODE:
        if (!sv_isobject(target) || !SvROK(target))
            croak("cancellation target must be an object");
        future = leaf_from_sv(future_obj);
        if (future->state == LEAF_CANCELLED) {
            dSP;
            ENTER;
            SAVETMPS;
            PUSHMARK(SP);
            PUSHs(target);
            PUTBACK;
            call_method("cancel", G_DISCARD | G_VOID);
            FREETMPS;
            LEAVE;
        } else if (future->state == LEAF_PENDING) {
            if (!future->cancel_chain)
                future->cancel_chain = newAV();
            av_push(future->cancel_chain, newSVsv(target));
        }

SV *
cancel(future_obj)
    SV *future_obj
    PREINIT:
        leaf_future_t *future;
        AV *cancel_callbacks;
        AV *cancel_chain;
        SV *ready_callback;
        AV *ready_callbacks;
        SV *failure = NULL;
    CODE:
        future = leaf_from_sv(future_obj);
        if (future->state == LEAF_PENDING) {
            cancel_callbacks = future->cancel_callbacks;
            cancel_chain = future->cancel_chain;
            ready_callback = future->ready_callback;
            ready_callbacks = future->ready_callbacks;
            future->cancel_callbacks = NULL;
            future->cancel_chain = NULL;
            future->ready_callback = NULL;
            future->ready_callbacks = NULL;
            future->state = LEAF_CANCELLED;
            leaf_call_callbacks(aTHX_ cancel_callbacks, &failure);
            leaf_cancel_chain(aTHX_ cancel_chain, &failure);
            leaf_call_callback_catching(aTHX_ ready_callback, &failure);
            leaf_call_callbacks(aTHX_ ready_callbacks, &failure);
            if (failure)
                croak_sv(sv_2mortal(failure));
        }
        RETVAL = newSVsv(future_obj);
    OUTPUT:
        RETVAL

SV *
loop(future_obj)
    SV *future_obj
    PREINIT:
        leaf_future_t *future;
    CODE:
        future = leaf_from_sv(future_obj);
        RETVAL = future->loop_sv ? newSVsv(future->loop_sv) : &PL_sv_undef;
    OUTPUT:
        RETVAL

void
DESTROY(future_obj)
    SV *future_obj
    PREINIT:
        leaf_future_t *future;
    CODE:
        if (sv_isobject(future_obj) && SvROK(future_obj)) {
            future = INT2PTR(leaf_future_t *, SvIV((SV *)SvRV(future_obj)));
            if (future) {
                if (future->loop_sv) SvREFCNT_dec(future->loop_sv);
                if (future->result) SvREFCNT_dec(future->result);
                if (future->results) SvREFCNT_dec((SV *)future->results);
                if (future->failure) SvREFCNT_dec(future->failure);
                if (future->ready_callback) SvREFCNT_dec(future->ready_callback);
                if (future->ready_callbacks) SvREFCNT_dec((SV *)future->ready_callbacks);
                if (future->cancel_callbacks) SvREFCNT_dec((SV *)future->cancel_callbacks);
                if (future->cancel_chain) SvREFCNT_dec((SV *)future->cancel_chain);
                Safefree(future);
                sv_setiv(SvRV(future_obj), 0);
            }
        }

/* Reusable Stream receive awaitable state supplied through the consumer ABI. */
typedef struct lea_recv_ctx_s {
    const les_consumer_host_api_v1_t *host;
    void *host_context;
    SV *result;
    SV *failure;
    SV *terminal_failure;
    SV *on_ready;
    SV *on_cancel;
    int armed;
    int ready;
    int cancelled;
    int in_delivery;
    int terminal;
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

static void
lea_clear_sv(SV **slot)
{
    if (*slot) {
        SvREFCNT_dec(*slot);
        *slot = NULL;
    }
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
    if (callback)
        lea_call_owned(aTHX_ ctx, callback);
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

    lea_fire_ready(aTHX_ ctx);
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
PROTOTYPES: DISABLE

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

        ctx->cancelled = 0;
        lea_clear_sv(&ctx->on_ready);
        lea_clear_sv(&ctx->on_cancel);

        if (ctx->terminal) {
            lea_prepare_terminal_result(ctx);
            XSRETURN_EMPTY;
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
