#include "EXTERN.h"
#include "perl.h"
#include "XSUB.h"

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
    SV *cancel_target;
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

static const char *
leaf_cancel_method(SV *target)
{
    HV *stash;

    if (!target || !sv_isobject(target) || !SvROK(target))
        return NULL;
    stash = SvSTASH(SvRV(target));
    if (stash && gv_fetchmethod_autoload(stash, "cancel", 0))
        return "cancel";
    if (stash && gv_fetchmethod_autoload(stash, "cancel_recv", 0))
        return "cancel_recv";
    return NULL;
}

static void
leaf_cancel_target(pTHX_ SV *target, SV **failure)
{
    const char *method = leaf_cancel_method(target);
    dSP;

    if (!method)
        return;
    ENTER;
    SAVETMPS;
    PUSHMARK(SP);
    PUSHs(target);
    PUTBACK;
    call_method(method, G_DISCARD | G_VOID | G_EVAL);
    if (SvTRUE(ERRSV)) {
        if (!*failure)
            *failure = newSVsv(ERRSV);
        sv_setsv(ERRSV, &PL_sv_undef);
    }
    FREETMPS;
    LEAVE;
}

static int
leaf_same_cancel_target(SV *left, SV *right)
{
    if (left == right)
        return 1;
    return SvROK(left) && SvROK(right) && SvRV(left) == SvRV(right);
}

static void
leaf_add_cancel_target(leaf_future_t *future, SV *target)
{
    SSize_t count;
    SV **last;

    if (!future->cancel_target) {
        future->cancel_target = newSVsv(target);
        return;
    }
    if (leaf_same_cancel_target(future->cancel_target, target))
        return;
    if (!future->cancel_chain)
        future->cancel_chain = newAV();
    count = av_count(future->cancel_chain);
    last = count ? av_fetch(future->cancel_chain, count - 1, 0) : NULL;
    if (last && *last && leaf_same_cancel_target(*last, target))
        return;
    av_push(future->cancel_chain, newSVsv(target));
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
        if (target && *target)
            leaf_cancel_target(aTHX_ *target, failure);
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
    if (future->cancel_target) {
        SvREFCNT_dec(future->cancel_target);
        future->cancel_target = NULL;
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

MODULE = Linux::Event::Async::Future    PACKAGE = Linux::Event::Async::Future
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
        SV *failure = NULL;
    CODE:
        if (!sv_isobject(target) || !SvROK(target))
            croak("cancellation target must be an object");
        future = leaf_from_sv(future_obj);
        if (future->state == LEAF_CANCELLED) {
            leaf_cancel_target(aTHX_ target, &failure);
            if (failure)
                croak_sv(sv_2mortal(failure));
        } else if (future->state == LEAF_PENDING) {
            leaf_add_cancel_target(future, target);
        }

SV *
cancel(future_obj)
    SV *future_obj
    PREINIT:
        leaf_future_t *future;
        AV *cancel_callbacks;
        SV *cancel_target;
        AV *cancel_chain;
        SV *ready_callback;
        AV *ready_callbacks;
        SV *failure = NULL;
    CODE:
        future = leaf_from_sv(future_obj);
        if (future->state == LEAF_PENDING) {
            cancel_callbacks = future->cancel_callbacks;
            cancel_target = future->cancel_target;
            cancel_chain = future->cancel_chain;
            ready_callback = future->ready_callback;
            ready_callbacks = future->ready_callbacks;
            future->cancel_callbacks = NULL;
            future->cancel_target = NULL;
            future->cancel_chain = NULL;
            future->ready_callback = NULL;
            future->ready_callbacks = NULL;
            future->state = LEAF_CANCELLED;
            leaf_call_callbacks(aTHX_ cancel_callbacks, &failure);
            if (cancel_target) {
                leaf_cancel_target(aTHX_ cancel_target, &failure);
                SvREFCNT_dec(cancel_target);
            }
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
                if (future->cancel_target) SvREFCNT_dec(future->cancel_target);
                if (future->cancel_chain) SvREFCNT_dec((SV *)future->cancel_chain);
                Safefree(future);
                sv_setiv(SvRV(future_obj), 0);
            }
        }
