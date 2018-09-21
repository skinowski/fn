package agent

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"strings"
	"sync"
	"time"

	"path/filepath"

	"github.com/fnproject/fn/api/agent/drivers"
	"github.com/fnproject/fn/api/agent/protocol"
	"github.com/fnproject/fn/api/common"
	"github.com/fnproject/fn/api/id"
	"github.com/fnproject/fn/api/models"
	"github.com/fnproject/fn/fnext"
	"github.com/fsnotify/fsnotify"
	docker "github.com/fsouza/go-dockerclient"
	"github.com/sirupsen/logrus"
	"go.opencensus.io/stats"
	"go.opencensus.io/trace"
	"os"
)

// TODO we should prob store async calls in db immediately since we're returning id (will 404 until post-execution)
// TODO async calls need to add route.Headers as well
// TODO handle timeouts / no response in sync & async (sync is json+503 atm, not 504, async is empty log+status)
// see also: server/runner.go wrapping the response writer there, but need to handle async too (push down?)
// TODO storing logs / call can push call over the timeout
// TODO async is still broken, but way less so. we need to modify mq semantics
// to be much more robust. now we're at least running it if we delete the msg,
// but we may never store info about that execution so still broked (if fn
// dies). need coordination w/ db.
// TODO if a cold call times out but container is created but hasn't replied, could
// end up that the client doesn't get a reply until long after the timeout (b/c of container removal, async it?)
// TODO if async would store requests (or interchange format) it would be slick, but
// if we're going to store full calls in db maybe we should only queue pointers to ids?
// TODO examine cases where hot can't start a container and the user would never see an error
// about why that may be so (say, whatever it is takes longer than the timeout, e.g.)
// TODO if an image is not found or similar issues in getting a slot, then async should probably
// mark the call as errored rather than forever trying & failing to run it
// TODO it would be really nice if we made the ramToken wrap the driver cookie (less brittle,
// if those leak the container leaks too...) -- not the allocation, but the token.Close and cookie.Close
// TODO if machine is out of ram, just timeout immediately / wait for hot slot? (discuss policy)

// Agent exposes an api to create calls from various parameters and then submit
// those calls, it also exposes a 'safe' shutdown mechanism via its Close method.
// Agent has a few roles:
//	* manage the memory pool for a given server
//	* manage the container lifecycle for calls (hot+cold)
//	* execute calls against containers
//	* invoke Start and End for each call appropriately
//	* check the mq for any async calls, and submit them
//
// Overview:
// Upon submission of a call, Agent will start the call's timeout timer
// immediately. If the call is hot, Agent will attempt to find an active hot
// container for that route, and if necessary launch another container. Cold
// calls will launch one container each. Cold calls will get container input
// and output directly, whereas hot calls will be able to read/write directly
// from/to a pipe in a container via Dispatch. If it's necessary to launch a
// container, first an attempt will be made to try to reserve the ram required
// while waiting for any hot 'slot' to become available [if applicable]. If
// there is an error launching the container, an error will be returned
// provided the call has not yet timed out or found another hot 'slot' to
// execute in [if applicable]. call.Start will be called immediately before
// starting a container, if cold (i.e. after pulling), or immediately before
// sending any input, if hot. call.End will be called regardless of the
// timeout timer's status if the call was executed, and that error returned may
// be returned from Submit.
type Agent interface {
	// GetCall will return a Call that is executable by the Agent, which
	// can be built via various CallOpt's provided to the method.
	GetCall(...CallOpt) (Call, error)

	// Submit will attempt to execute a call locally, a Call may store information
	// about itself in its Start and End methods, which will be called in Submit
	// immediately before and after the Call is executed, respectively. An error
	// will be returned if there is an issue executing the call or the error
	// may be from the call's execution itself (if, say, the container dies,
	// or the call times out).
	Submit(Call) error

	// Close will wait for any outstanding calls to complete and then exit.
	// Closing the agent will invoke Close on the underlying DataAccess.
	// Close is not safe to be called from multiple threads.
	io.Closer

	AddCallListener(fnext.CallListener)
}

type agent struct {
	cfg           Config
	da            CallHandler
	callListeners []fnext.CallListener

	driver drivers.Driver

	slotMgr *slotQueueMgr
	evictor Evictor
	// track usage
	resources ResourceTracker

	// used to track running calls / safe shutdown
	shutWg              *common.WaitGroup
	shutonce            sync.Once
	disableAsyncDequeue bool

	callOverrider CallOverrider
	// deferred actions to call at end of initialisation
	onStartup []func()
}

// Option configures an agent at startup
type Option func(*agent) error

// RegistryToken is a reserved call extensions key to pass registry token
const RegistryToken = "FN_REGISTRY_TOKEN"

// New creates an Agent that executes functions locally as Docker containers.
func New(da CallHandler, options ...Option) Agent {

	cfg, err := NewConfig()
	if err != nil {
		logrus.WithError(err).Fatalf("error in agent config cfg=%+v", cfg)
	}

	a := &agent{
		cfg: *cfg,
	}

	a.shutWg = common.NewWaitGroup()
	a.da = da
	a.slotMgr = NewSlotQueueMgr()
	a.evictor = NewEvictor()

	// Allow overriding config
	for _, option := range options {
		err = option(a)
		if err != nil {
			logrus.WithError(err).Fatalf("error in agent options")
		}
	}

	logrus.Infof("agent starting cfg=%+v", a.cfg)

	if a.driver == nil {
		d, err := NewDockerDriver(&a.cfg)
		if err != nil {
			logrus.WithError(err).Fatal("failed to create docker driver ")
		}
		a.driver = d
	}

	a.resources = NewResourceTracker(&a.cfg)

	for _, sup := range a.onStartup {
		sup()
	}
	return a
}

func (a *agent) addStartup(sup func()) {
	a.onStartup = append(a.onStartup, sup)

}

// WithAsync Enables Async  operations on the agent
func WithAsync(dqda DequeueDataAccess) Option {
	return func(a *agent) error {
		if !a.shutWg.AddSession(1) {
			logrus.Fatalf("cannot start agent, unable to add session")
		}
		a.addStartup(func() {
			go a.asyncDequeue(dqda) // safe shutdown can nanny this fine
		})
		return nil
	}
}

// WithConfig sets the agent config to the provided config
func WithConfig(cfg *Config) Option {
	return func(a *agent) error {
		a.cfg = *cfg
		return nil
	}
}

// WithDockerDriver Provides a customer driver to agent
func WithDockerDriver(drv drivers.Driver) Option {
	return func(a *agent) error {
		if a.driver != nil {
			return errors.New("cannot add driver to agent, driver already exists")
		}

		a.driver = drv
		return nil
	}
}

// WithCallOverrider registers register a CallOverrider to modify a Call and extensions on call construction
func WithCallOverrider(fn CallOverrider) Option {
	return func(a *agent) error {
		if a.callOverrider != nil {
			return errors.New("lb-agent call overriders already exists")
		}
		a.callOverrider = fn
		return nil
	}
}

// NewDockerDriver creates a default docker driver from agent config
func NewDockerDriver(cfg *Config) (drivers.Driver, error) {
	return drivers.New("docker", drivers.Config{
		DockerNetworks:       cfg.DockerNetworks,
		DockerLoadFile:       cfg.DockerLoadFile,
		ServerVersion:        cfg.MinDockerVersion,
		PreForkPoolSize:      cfg.PreForkPoolSize,
		PreForkImage:         cfg.PreForkImage,
		PreForkCmd:           cfg.PreForkCmd,
		PreForkUseOnce:       cfg.PreForkUseOnce,
		PreForkNetworks:      cfg.PreForkNetworks,
		MaxTmpFsInodes:       cfg.MaxTmpFsInodes,
		EnableReadOnlyRootFs: !cfg.DisableReadOnlyRootFs,
		EnableTini:           !cfg.DisableTini,
	})
}

func (a *agent) Close() error {
	var err error

	// wait for ongoing sessions
	a.shutWg.CloseGroup()

	a.shutonce.Do(func() {
		// now close docker layer
		if a.driver != nil {
			err = a.driver.Close()
		}
	})

	return err
}

func (a *agent) Submit(callI Call) error {
	call := callI.(*call)
	ctx, span := trace.StartSpan(call.req.Context(), "agent_submit")
	defer span.End()

	statsCalls(ctx)

	if !a.shutWg.AddSession(1) {
		statsTooBusy(ctx)
		return models.ErrCallTimeoutServerBusy
	}
	defer a.shutWg.DoneSession()

	err := a.submit(ctx, call)
	return err
}

func (a *agent) startStateTrackers(ctx context.Context, call *call) {

	if !protocol.IsStreamable(protocol.Protocol(call.Format)) {
		// For cold containers, we track the container state in call
		call.containerState = NewContainerState()
	}

	call.requestState = NewRequestState()
}

func (a *agent) endStateTrackers(ctx context.Context, call *call) {

	call.requestState.UpdateState(ctx, RequestStateDone, call.slots)

	// For cold containers, we are done with the container.
	if call.containerState != nil {
		call.containerState.UpdateState(ctx, ContainerStateDone, call.slots)
	}
}

func (a *agent) submit(ctx context.Context, call *call) error {
	statsEnqueue(ctx)

	a.startStateTrackers(ctx, call)
	defer a.endStateTrackers(ctx, call)

	slot, err := a.getSlot(ctx, call)
	if err != nil {
		return a.handleCallEnd(ctx, call, slot, err, false)
	}

	err = call.Start(ctx)
	if err != nil {
		return a.handleCallEnd(ctx, call, slot, err, false)
	}

	statsDequeue(ctx)
	statsStartRun(ctx)

	// We are about to execute the function, set container Exec Deadline (call.Timeout)
	slotCtx, cancel := context.WithTimeout(ctx, time.Duration(call.Timeout)*time.Second)
	defer cancel()

	// Pass this error (nil or otherwise) to end directly, to store status, etc.
	err = slot.exec(slotCtx, call)
	return a.handleCallEnd(ctx, call, slot, err, true)
}

func (a *agent) handleCallEnd(ctx context.Context, call *call, slot Slot, err error, isStarted bool) error {

	if slot != nil {
		slot.Close()
	}

	// This means call was routed (executed)
	if isStarted {
		call.End(ctx, err)
		statsStopRun(ctx)
		if err == nil {
			statsComplete(ctx)
		}
	} else {
		statsDequeue(ctx)
		if err == models.ErrCallTimeoutServerBusy || err == context.DeadlineExceeded {
			statsTooBusy(ctx)
			return models.ErrCallTimeoutServerBusy
		}
	}

	if err == context.DeadlineExceeded {
		statsTimedout(ctx)
		return models.ErrCallTimeout
	} else if err == context.Canceled {
		statsCanceled(ctx)
	} else if err != nil {
		statsErrors(ctx)
	}
	return err
}

// getSlot returns a Slot (or error) for the request to run. Depending on hot/cold
// request type, this may launch a new container or wait for other containers to become idle
// or it may wait for resources to become available to launch a new container.
func (a *agent) getSlot(ctx context.Context, call *call) (Slot, error) {
	if call.Type == models.TypeAsync {
		// *) for async, slot deadline is also call.Timeout. This is because we would like to
		// allocate enough time for docker-pull, slot-wait, docker-start, etc.
		// and also make sure we have call.Timeout inside the container. Total time
		// to run an async becomes 2 * call.Timeout.
		// *) for sync, there's no slot deadline, the timeout is controlled by http-client
		// context (or runner gRPC context)
		tmp, cancel := context.WithTimeout(ctx, time.Duration(call.Timeout)*time.Second)
		ctx = tmp
		defer cancel()
	}

	ctx, span := trace.StartSpan(ctx, "agent_get_slot")
	defer span.End()

	if protocol.IsStreamable(protocol.Protocol(call.Format)) {
		if call.slotHashId == "" {
			call.slotHashId = getSlotQueueKey(call)
		}

		call.slots, _ = a.slotMgr.getSlotQueue(call.slotHashId)
		call.requestState.UpdateState(ctx, RequestStateWait, call.slots)
		return a.waitHot(ctx, call)
	}

	call.requestState.UpdateState(ctx, RequestStateWait, call.slots)
	return a.launchCold(ctx, call)
}

// checkLaunch monitors both slot queue and resource tracker to get a token or a new
// container whichever is faster. If a new container is launched, checkLaunch will wait
// until launch completes before trying to spawn more containers.
func (a *agent) checkLaunch(ctx context.Context, call *call, slotChan chan *slotToken) (Slot, error) {

	isAsync := call.Type == models.TypeAsync
	mem := call.Memory + uint64(call.TmpFsSize)

	var launchPending chan struct{}
	var resourceChan <-chan ResourceToken
	var cancel func()

	defer func() {
		if cancel != nil {
			cancel()
		}
	}()

	// Initial quick check to drain any easy tokens in slot queue. Very happy case.
	s, err := a.tryGetToken(ctx, call, slotChan)
	if s != nil || err != nil {
		return s, err
	}

	for {
		// If we are currently launching a container, let's wait for the launch process.
		// We also check slotChan in case a previously busy container gets done earlier
		// than the current launch process. This cuts down latency in those cases, but
		// leaves an orphan container, which can be used by other queries or claimed by
		// evictor
		if launchPending != nil {
			select {
			case s := <-slotChan:
				// check to see if slot is still valid (could be evicted, exited, etc.)
				if call.slots.acquireSlot(s) {
					if s.slot.Error() != nil {
						s.slot.Close()
						return nil, s.slot.Error()
					}
					return s.slot, nil
				}
			case <-ctx.Done():
				return nil, ctx.Err()
			case <-a.shutWg.Closer(): // server shutdown
				return nil, models.ErrCallTimeoutServerBusy
			case <-launchPending:
				launchPending = nil
			}
			if launchPending != nil {
				// we are here because we got a token but couldn't acquire it.
				continue
			}
		}

		// IMPORTANT: Check again here to avoid select below to randomly
		// pick resourceChan if token is already available. Remember if
		// multiple channels are I/O pending, then select will pick at random.
		// Extra poll here prioritizes slotChan slightly
		s, err := a.tryGetToken(ctx, call, slotChan)
		if s != nil || err != nil {
			return s, err
		}

		// Get a resourceChan if we do not have one. Otherwise, select below
		// will continue with previous resourceChan.
		if resourceChan == nil {
			ctx, ctxCancel := context.WithCancel(ctx)
			cancel = ctxCancel
			resourceChan = a.resources.GetResourceToken(ctx, mem, call.CPUs, isAsync)
		}

		select {
		case s := <-slotChan:
			// check to see if slot is still valid (could be evicted, exited, etc.)
			if call.slots.acquireSlot(s) {
				if s.slot.Error() != nil {
					s.slot.Close()
					return nil, s.slot.Error()
				}
				return s.slot, nil
			}
		case resource := <-resourceChan:
			statsUtilization(ctx, a.resources.GetUtilization())

			// We received our resource, now shutdown GetResourceToken() channel via cancel
			// and clear the resourceChan. resourceChan is one-time use only with one resource
			// output.
			cancel()
			cancel = nil
			resourceChan = nil

			// If we can't add a session, we are shutting down, return too-busy
			if !a.shutWg.AddSession(1) {
				resource.Close()
				return nil, models.ErrCallTimeoutServerBusy
			}

			// set a new launchPending channel, which we will wait on before attempting
			// to launch more containers.
			launchPending = make(chan struct{}, 1)
			go func() {
				// NOTE: runHot will not inherit the timeout from ctx (ignore timings)
				a.runHot(ctx, call, resource, launchPending)
				a.shutWg.DoneSession()
			}()
		case <-time.After(a.cfg.HotPoll):
			// We are here because we waited for too long for CPU. We evict and continue waiting for resources
			if !a.evictor.PerformEviction(call.slotHashId, mem, uint64(call.CPUs)) && a.cfg.EnableNBResourceTracker {
				return nil, models.ErrCallTimeoutServerBusy
			}
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-a.shutWg.Closer(): // server shutdown
			return nil, models.ErrCallTimeoutServerBusy
		}
	}
}

// tryGetToken attempts to fetch/acquire a token from slot queue without blocking.
func (a *agent) tryGetToken(ctx context.Context, call *call, ch chan *slotToken) (Slot, error) {
	for {
		select {
		case s := <-ch:
			if call.slots.acquireSlot(s) {
				if s.slot.Error() != nil {
					s.slot.Close()
					return nil, s.slot.Error()
				}
				return s.slot, nil
			}
		default:
			return nil, nil
		}
	}
}

// waitHot waits for a hot container or launches a new one
func (a *agent) waitHot(ctx context.Context, call *call) (Slot, error) {
	ctx, span := trace.StartSpan(ctx, "agent_wait_hot")
	defer span.End()

	ctx, cancel := context.WithCancel(ctx)
	defer cancel() // shut down dequeuer if we grab a slot

	ch := call.slots.startDequeuer(ctx)
	return a.checkLaunch(ctx, call, ch)
}

// launchCold waits for necessary resources to launch a new container, then
// returns the slot for that new container to run the request on.
func (a *agent) launchCold(ctx context.Context, call *call) (Slot, error) {
	ctx, span := trace.StartSpan(ctx, "agent_launch_cold")
	defer span.End()

	isAsync := call.Type == models.TypeAsync
	ch := make(chan Slot)
	mem := call.Memory + uint64(call.TmpFsSize)

	select {
	case tok := <-a.resources.GetResourceToken(ctx, mem, call.CPUs):
		go a.prepCold(ctx, call, tok, ch)
	case <-ctx.Done():
		return nil, ctx.Err()
	}

	// wait for launch err or a slot to open up
	select {
	case s := <-ch:
		if s.Error() != nil {
			s.Close()
			return nil, s.Error()
		}
		return s, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

// implements Slot
type coldSlot struct {
	cookie   drivers.Cookie
	tok      ResourceToken
	closer   func()
	fatalErr error
}

func (s *coldSlot) Error() error {
	return s.fatalErr
}

func (s *coldSlot) exec(ctx context.Context, call *call) error {
	ctx, span := trace.StartSpan(ctx, "agent_cold_exec")
	defer span.End()

	call.requestState.UpdateState(ctx, RequestStateExec, call.slots)
	call.containerState.UpdateState(ctx, ContainerStateBusy, call.slots)

	waiter, err := s.cookie.Run(ctx)
	if err != nil {
		return err
	}

	res := waiter.Wait(ctx)
	if res.Error() != nil {
		// check for call error (oom/exit) and beam it up
		return res.Error()
	}

	// nil or timed out
	return ctx.Err()
}

func (s *coldSlot) Close() error {
	if s.closer != nil {
		s.closer()
		s.closer = nil
	}
	return nil
}

// implements Slot
type hotSlot struct {
	done          chan struct{} // signal we are done with slot
	errC          <-chan error  // container error
	container     *container    // TODO mask this
	cfg           *Config
	udsClient     http.Client
	fatalErr      error
	containerSpan trace.SpanContext
}

func (s *hotSlot) Close() error {
	close(s.done)
	return nil
}

func (s *hotSlot) Error() error {
	return s.fatalErr
}

func (s *hotSlot) trySetError(err error) {
	if s.fatalErr == nil {
		s.fatalErr = err
	}
}

func (s *hotSlot) exec(ctx context.Context, call *call) error {
	ctx, span := trace.StartSpan(ctx, "agent_hot_exec")
	defer span.End()

	call.requestState.UpdateState(ctx, RequestStateExec, call.slots)

	// link the container id and id in the logs [for us!]
	common.Logger(ctx).WithField("container_id", s.container.id).Info("starting call")

	// link the container span to ours for additional context (start/freeze/etc.)
	span.AddLink(trace.Link{
		TraceID: s.containerSpan.TraceID,
		SpanID:  s.containerSpan.SpanID,
		Type:    trace.LinkTypeChild,
	})

	call.req = call.req.WithContext(ctx) // TODO this is funny biz reed is bad

	var errApp chan error
	if call.Format == models.FormatHTTPStream {
		errApp = s.dispatch(ctx, call)
	} else { // TODO remove this block one glorious day
		errApp = s.dispatchOldFormats(ctx, call)
	}

	select {
	case err := <-s.errC: // error from container
		s.trySetError(err)
		return err
	case err := <-errApp: // from dispatch
		if err != nil {
			if models.IsAPIError(err) {
				s.trySetError(err)
			} else if err == protocol.ErrExcessData {
				s.trySetError(err)
				// suppress excess data error, but do shutdown the container
				return nil
			}
		}
		return err
	case <-ctx.Done(): // call timeout
		s.trySetError(ctx.Err())
		return ctx.Err()
	}
}

var removeHeaders = map[string]bool{
	"connection":        true,
	"keep-alive":        true,
	"trailer":           true,
	"transfer-encoding": true,
	"te":                true,
	"upgrade":           true,
	"authorization":     true,
}

func callToHTTPRequest(ctx context.Context, call *call) (*http.Request, error) {
	req, err := http.NewRequest("POST", "http://localhost/call", call.req.Body)
	if err != nil {
		return req, err
	}
	// Set the context on the request to make sure transport and client handle
	// it properly and close connections at the end, e.g. when using UDS.
	req = req.WithContext(ctx)

	req.Header = make(http.Header)
	for k, vs := range call.req.Header {
		if !removeHeaders[strings.ToLower(k)] {
			for _, v := range vs {
				req.Header.Add(k, v)
			}
		}
	}

	//req.Header.Set("FN_DEADLINE", ci.Deadline().String())
	// TODO(occ) : fix compatidupes when FDKs are updated
	req.Header.Set("Fn-Call-Id", call.ID)
	req.Header.Set("FN_CALL_ID", call.ID)
	deadline, ok := ctx.Deadline()
	if ok {
		deadlineStr := deadline.Format(time.RFC3339)
		req.Header.Set("Fn-Deadline", deadlineStr)
		req.Header.Set("FN_DEADLINE", deadlineStr)
	}

	return req, err
}

func (s *hotSlot) dispatch(ctx context.Context, call *call) chan error {
	ctx, span := trace.StartSpan(ctx, "agent_dispatch_httpstream")
	defer span.End()

	// TODO we can't trust that resp.Write doesn't timeout, even if the http
	// client should respect the request context (right?) so we still need this (right?)
	errApp := make(chan error, 1)

	req, err := callToHTTPRequest(ctx, call)

	if err != nil {
		errApp <- err
		return errApp
	}

	go func() {
		// TODO it's possible we can get rid of this (after getting rid of logs API) - may need for call id/debug mode still
		// TODO there's a timeout race for swapping this back if the container doesn't get killed for timing out, and don't you forget it
		swapBack := s.container.swap(nil, call.stderr, call.stderr, &call.Stats)
		defer swapBack()

		resp, err := s.udsClient.Do(req)
		if err != nil {
			common.Logger(ctx).WithError(err).Error("Got error from UDS socket")
			errApp <- models.NewAPIError(http.StatusBadGateway, errors.New("error receiving function response"))
			return
		}
		common.Logger(ctx).WithField("resp", resp).Debug("Got resp from UDS socket")

		defer resp.Body.Close()

		select {
		case errApp <- writeResp(s.cfg.MaxResponseSize, resp, call.w):
		case <-ctx.Done():
			errApp <- ctx.Err()
		}
	}()
	return errApp
}

// XXX(reed): dupe code in http proto (which will die...)
func writeResp(max uint64, resp *http.Response, w io.Writer) error {
	rw, ok := w.(http.ResponseWriter)
	if !ok {
		w = common.NewClampWriter(rw, max, models.ErrFunctionResponseTooBig)
		return resp.Write(w)
	}

	rw = newSizerRespWriter(max, rw)

	// if we're writing directly to the response writer, we need to set headers
	// and status code, and only copy the body. resp.Write would copy a full
	// http request into the response body (not what we want).

	for k, vs := range resp.Header {
		for _, v := range vs {
			rw.Header().Add(k, v)
		}
	}
	if resp.StatusCode > 0 {
		rw.WriteHeader(resp.StatusCode)
	}
	_, err := io.Copy(rw, resp.Body)
	return err
}

// XXX(reed): this is a remnant of old io.pipe plumbing, we need to get rid of
// the buffers from the front-end in actuality, but only after removing other formats... so here, eat this
type sizerRespWriter struct {
	http.ResponseWriter
	w io.Writer
}

var _ http.ResponseWriter = new(sizerRespWriter)

func newSizerRespWriter(max uint64, rw http.ResponseWriter) http.ResponseWriter {
	return &sizerRespWriter{
		ResponseWriter: rw,
		w:              common.NewClampWriter(rw, max, models.ErrFunctionResponseTooBig),
	}
}

func (s *sizerRespWriter) Write(b []byte) (int, error) { return s.w.Write(b) }

// TODO remove
func (s *hotSlot) dispatchOldFormats(ctx context.Context, call *call) chan error {

	errApp := make(chan error, 1)
	go func() {
		// XXX(reed): this may be liable to leave the pipes fucked up if dispatch times out, eg
		// we may need ye ole close() func to put the Close()/swapBack() in from the caller

		// swap in fresh pipes & stat accumulator to not interlace with other calls that used this slot [and timed out]
		stdinRead, stdinWrite := io.Pipe()
		stdoutRead, stdoutWritePipe := io.Pipe()
		defer stdinRead.Close()
		defer stdoutWritePipe.Close()

		// NOTE: stderr is limited separately (though line writer is vulnerable to attack?)
		// limit the bytes allowed to be written to the stdout pipe, which handles any
		// buffering overflows (json to a string, http to a buffer, etc)
		stdoutWrite := common.NewClampWriter(stdoutWritePipe, s.cfg.MaxResponseSize, models.ErrFunctionResponseTooBig)

		swapBack := s.container.swap(stdinRead, stdoutWrite, call.stderr, &call.Stats)
		defer swapBack() // NOTE: it's important this runs before the pipes are closed.

		// TODO this should get killed completely
		// TODO we could alternatively dial in and use the conn as stdin/stdout for an interim solution
		// XXX(reed): ^^^ do we need that for the cloud event dance ????
		proto := protocol.New(protocol.Protocol(call.Format), stdinWrite, stdoutRead)
		ci := protocol.NewCallInfo(call.IsCloudEvent, call.Call, call.req)
		errApp <- proto.Dispatch(ctx, ci, call.w)
	}()

	return errApp
}

func (a *agent) prepCold(ctx context.Context, call *call, tok ResourceToken, ch chan Slot) {
	ctx, span := trace.StartSpan(ctx, "agent_prep_cold")
	defer span.End()
	statsUtilization(ctx, a.resources.GetUtilization())

	call.containerState.UpdateState(ctx, ContainerStateStart, call.slots)

	deadline := time.Now().Add(time.Duration(call.Timeout) * time.Second)

	// add Fn-specific information to the config to shove everything into env vars for cold
	call.Config["FN_DEADLINE"] = common.DateTime(deadline).String()
	call.Config["FN_METHOD"] = call.Model().Method
	call.Config["FN_REQUEST_URL"] = call.Model().URL
	call.Config["FN_CALL_ID"] = call.Model().ID

	// User headers are prefixed with FN_HEADER and shoved in the env vars too
	for k, v := range call.Headers {
		k = "FN_HEADER_" + k
		call.Config[k] = strings.Join(v, ", ")
	}

	container := &container{
		id:         id.New().String(), // XXX we could just let docker generate ids...
		image:      call.Image,
		env:        map[string]string(call.Config),
		extensions: call.extensions,
		memory:     call.Memory,
		cpus:       uint64(call.CPUs),
		fsSize:     a.cfg.MaxFsSize,
		iofs:       &noopIOFS{},
		timeout:    time.Duration(call.Timeout) * time.Second, // this is unnecessary, but in case removal fails...
		logCfg: drivers.LoggerConfig{
			URL: strings.TrimSpace(call.SyslogURL),
			Tags: []drivers.LoggerTag{
				{Name: "app_id", Value: call.AppID},
				{Name: "fn_id", Value: call.FnID},
			},
		},
		stdin:  call.req.Body,
		stdout: common.NewClampWriter(call.w, a.cfg.MaxResponseSize, models.ErrFunctionResponseTooBig),
		stderr: call.stderr,
		stats:  &call.Stats,
	}

	cookie, err := a.driver.CreateCookie(ctx, container)
	if err == nil {
		// pull & create container before we return a slot, so as to be friendly
		// about timing out if this takes a while...
		err = a.driver.PrepareCookie(ctx, cookie)
	}

	call.containerState.UpdateState(ctx, ContainerStateIdle, call.slots)

	closer := func() {
		if cookie != nil {
			cookie.Close(ctx)
		}
		if tok != nil {
			tok.Close()
		}
		statsUtilization(ctx, a.resources.GetUtilization())
	}

	slot := &coldSlot{cookie: cookie, tok: tok, closer: closer, fatalErr: err}
	select {
	case ch <- slot:
	case <-ctx.Done():
		slot.Close()
	}
}

func (a *agent) runHot(ctx context.Context, call *call, tok ResourceToken, ready chan struct{}) {
	// IMPORTANT: get a context that has a child span / logger but NO timeout
	// TODO this is a 'FollowsFrom'
	ctx = common.BackgroundContext(ctx)
	ctx, span := trace.StartSpan(ctx, "agent_run_hot")
	defer span.End()

	state := NewContainerState()

	defer func() {
		select {
		case ready <- struct{}{}:
		default:
		}
		statsUtilization(ctx, a.resources.GetUtilization())
	}()

	defer tok.Close() // IMPORTANT: this MUST get called

	state.UpdateState(ctx, ContainerStateStart, call.slots)
	defer state.UpdateState(ctx, ContainerStateDone, call.slots)

	container, err := newHotContainer(ctx, call, &a.cfg)
	if err != nil {
		call.slots.queueSlot(&hotSlot{done: make(chan struct{}), fatalErr: err})
		return
	}
	defer container.Close()

	// NOTE: soon this isn't assigned in a branch...
	var udsClient http.Client
	udsAwait := make(chan error)
	if call.Format == models.FormatHTTPStream {
		// start our listener before starting the container, so we don't miss the pretty things whispered in our ears
		go inotifyUDS(ctx, container.UDSAgentPath(), udsAwait)

		udsClient = http.Client{
			Transport: &http.Transport{
				MaxIdleConns:        1,
				MaxIdleConnsPerHost: 1,
				// XXX(reed): other settings ?
				IdleConnTimeout: 1 * time.Second,
				DialContext: func(ctx context.Context, _, _ string) (net.Conn, error) {
					var d net.Dialer
					return d.DialContext(ctx, "unix", filepath.Join(container.UDSAgentPath(), udsFilename))
				},
			},
		}
	} else {
		close(udsAwait) // XXX(reed): short case first / kill this
	}

	logger := logrus.WithFields(logrus.Fields{"id": container.id, "app_id": call.AppID, "fn_id": call.FnID, "image": call.Image, "memory": call.Memory, "cpus": call.CPUs, "format": call.Format, "idle_timeout": call.IdleTimeout})
	ctx = common.WithLogger(ctx, logger)

	cookie, err := a.driver.CreateCookie(ctx, container)
	if err != nil {
		call.slots.queueSlot(&hotSlot{done: make(chan struct{}), fatalErr: err})
		return
	}

	defer cookie.Close(ctx)

	err = a.driver.PrepareCookie(ctx, cookie)
	if err != nil {
		call.slots.queueSlot(&hotSlot{done: make(chan struct{}), fatalErr: err})
		return
	}

	waiter, err := cookie.Run(ctx)
	if err != nil {
		call.slots.queueSlot(&hotSlot{done: make(chan struct{}), fatalErr: err})
		return
	}

	// buffered, in case someone has slot when waiter returns but isn't yet listening
	errC := make(chan error, 1)

	ctx, shutdownContainer := context.WithCancel(ctx)
	defer shutdownContainer() // close this if our waiter returns, to call off slots
	go func() {
		defer shutdownContainer() // also close if we get an agent shutdown / idle timeout

		// now we wait for the socket to be created before handing out any slots, need this
		// here in case the container dies before making the sock we need to bail
		select {
		case err := <-udsAwait: // XXX(reed): need to leave a note about pairing ctx here?
			// sends a nil error if all is good, we can proceed...
			if err != nil {
				call.slots.queueSlot(&hotSlot{done: make(chan struct{}), fatalErr: err})
				return
			}

		case <-ctx.Done():
			call.slots.queueSlot(&hotSlot{done: make(chan struct{}), fatalErr: ctx.Err()})
			return
		}

		initSig := ready

		for {
			select { // make sure everything is up before trying to send slot
			case <-ctx.Done(): // container shutdown
				return
			case <-a.shutWg.Closer(): // server shutdown
				return
			default: // ok
			}

			slot := &hotSlot{
				done:          make(chan struct{}),
				errC:          errC,
				container:     container,
				cfg:           &a.cfg,
				udsClient:     udsClient,
				containerSpan: trace.FromContext(ctx).SpanContext(),
			}
			if !a.runHotReq(ctx, call, state, logger, cookie, slot, initSig) {
				return
			}
			// wait for this call to finish
			// NOTE do NOT select with shutdown / other channels. slot handles this.
			<-slot.done

			if slot.fatalErr != nil {
				logger.WithError(slot.fatalErr).Info("hot function terminating")
				return
			}

			initSig = nil
		}
	}()

	res := waiter.Wait(ctx)
	if res.Error() != nil {
		errC <- res.Error() // TODO: race condition, no guaranteed delivery fix this...
	}

	logger.WithError(res.Error()).Info("hot function terminated")
}

//checkSocketDestination verifies that the socket file created by the FDK is valid and permitted - notably verifying that any symlinks are relative to the socket dir
func checkSocketDestination(filename string) error {
	finfo, err := os.Lstat(filename)
	if err != nil {
		return fmt.Errorf("error statting unix socket link file %s", err)
	}

	if (finfo.Mode() & os.ModeSymlink) > 0 {
		linkDest, err := os.Readlink(filename)
		if err != nil {
			return fmt.Errorf("error reading unix socket symlink destination %s", err)
		}
		if filepath.Dir(linkDest) != "." {
			return fmt.Errorf("invalid unix socket symlink, symlinks must be relative within the unix socket directory")
		}
	}

	// stat the absolute path and check it is a socket
	absInfo, err := os.Stat(filename)
	if err != nil {
		return fmt.Errorf("unable to stat unix socket file %s", err)
	}
	if absInfo.Mode()&os.ModeSocket == 0 {
		return fmt.Errorf("listener file is not a socket")
	}

	return nil
}
func inotifyUDS(ctx context.Context, iofsDir string, awaitUDS chan<- error) {
	// XXX(reed): I forgot how to plumb channels temporarily forgive me for this sin (inotify will timeout, this is just bad programming)
	err := inotifyAwait(ctx, iofsDir)
	if err == nil {
		err = checkSocketDestination(filepath.Join(iofsDir, udsFilename))
	}
	select {
	case awaitUDS <- err:
	case <-ctx.Done():
	}
}

func inotifyAwait(ctx context.Context, iofsDir string) error {
	ctx, span := trace.StartSpan(ctx, "inotify_await")
	defer span.End()

	fsWatcher, err := fsnotify.NewWatcher()
	if err != nil {
		return fmt.Errorf("error getting fsnotify watcher: %v", err)
	}
	defer func() {
		if err := fsWatcher.Close(); err != nil {
			common.Logger(ctx).WithError(err).Error("Failed to close inotify watcher")
		}
	}()

	err = fsWatcher.Add(iofsDir)
	if err != nil {
		return fmt.Errorf("error adding iofs dir to fswatcher: %v", err)
	}

	for {
		select {
		case <-ctx.Done():
			// XXX(reed): damn it would sure be nice to tell users they didn't make a uds and that's why it timed out
			return ctx.Err()
		case err := <-fsWatcher.Errors:
			return fmt.Errorf("error watching for iofs: %v", err)
		case event := <-fsWatcher.Events:
			common.Logger(ctx).WithField("event", event).Debug("fsnotify event")
			if event.Op&fsnotify.Create == fsnotify.Create && event.Name == filepath.Join(iofsDir, udsFilename) {

				// wait until the socket file is created by the container
				return nil
			}
		}
	}
}

// runHotReq enqueues a free slot to slot queue manager and watches various timers and the consumer until
// the slot is consumed. A return value of false means, the container should shutdown and no subsequent
// calls should be made to this function.
func (a *agent) runHotReq(ctx context.Context, call *call, state ContainerState, logger logrus.FieldLogger, cookie drivers.Cookie, slot *hotSlot, initSig chan struct{}) bool {

	var err error
	isFrozen := false
	isEvictEvent := false

	freezeTimer := time.NewTimer(a.cfg.FreezeIdle)
	idleTimer := time.NewTimer(time.Duration(call.IdleTimeout) * time.Second)
	evictor := a.evictor.GetEvictor(call.ID, call.slotHashId, call.Memory+uint64(call.TmpFsSize), uint64(call.CPUs))

	defer func() {
		a.evictor.UnregisterEvictor(evictor)
		freezeTimer.Stop()
		idleTimer.Stop()
		// log if any error is encountered
		if err != nil {
			logger.WithError(err).Error("hot function failure")
		}
	}()

	a.evictor.RegisterEvictor(evictor)
	state.UpdateState(ctx, ContainerStateIdle, call.slots)

	s := call.slots.queueSlot(slot)
	if initSig != nil {
		select {
		case initSig <- struct{}{}:
		default:
		}
	}

	for {
		select {
		case <-s.trigger: // slot already consumed
		case <-ctx.Done(): // container shutdown
		case <-a.shutWg.Closer(): // server shutdown
		case <-idleTimer.C:
		case <-freezeTimer.C:
			if !isFrozen {
				err = cookie.Freeze(ctx)
				if err != nil {
					return false
				}
				isFrozen = true
				state.UpdateState(ctx, ContainerStatePaused, call.slots)
			}
			continue
		case <-evictor.C:
			logger.Debug("attempting hot function eviction")
			isEvictEvent = true
		}
		break
	}

	a.evictor.UnregisterEvictor(evictor)

	// if we can acquire token, that means we are here due to
	// abort/shutdown/timeout, attempt to acquire and terminate,
	// otherwise continue processing the request
	if call.slots.acquireSlot(s) {
		slot.Close()
		if isEvictEvent {
			statsContainerEvicted(ctx)
		}
		return false
	}

	// In case, timer/acquireSlot failure landed us here, make
	// sure to unfreeze.
	if isFrozen {
		err = cookie.Unfreeze(ctx)
		if err != nil {
			return false
		}
		isFrozen = false
	}

	state.UpdateState(ctx, ContainerStateBusy, call.slots)
	return true
}

// container implements drivers.ContainerTask container is the execution of a
// single container, which may run multiple functions [consecutively]. the id
// and stderr can be swapped out by new calls in the container.  input and
// output must be copied in and out.
type container struct {
	id         string // contrived
	image      string
	env        map[string]string
	extensions map[string]string
	memory     uint64
	cpus       uint64
	fsSize     uint64
	tmpFsSize  uint64
	iofs       iofs
	timeout    time.Duration // cold only (superfluous, but in case)
	logCfg     drivers.LoggerConfig
	close      func()

	stdin  io.Reader
	stdout io.Writer
	stderr io.Writer

	// swapMu protects the stats swapping
	swapMu sync.Mutex
	stats  *drivers.Stats
}

//newHotContainer creates a container that can be used for multiple sequential events
func newHotContainer(ctx context.Context, call *call, cfg *Config) (*container, error) {
	// if freezer is enabled, be consistent with freezer behavior and
	// block stdout and stderr between calls.
	isBlockIdleIO := MaxMsDisabled != cfg.FreezeIdle

	id := id.New().String()

	stdin := common.NewGhostReader()
	stderr := common.NewGhostWriter()
	stdout := common.NewGhostWriter()

	// for use if no freezer (or we ever make up our minds)
	var bufs []*bytes.Buffer

	// when not processing a request, do we block IO?
	if !isBlockIdleIO {
		// IMPORTANT: we are not operating on a TTY allocated container. This means, stderr and stdout are multiplexed
		// from the same stream internally via docker using a multiplexing protocol. Therefore, stderr/stdout *BOTH*
		// have to be read or *BOTH* blocked consistently. In other words, we cannot block one and continue
		// reading from the other one without risking head-of-line blocking.

		// wrap the syslog and debug loggers in the same (respective) line writer
		// syslog complete chain for this (from top):
		// stderr -> line writer

		// TODO(reed): I guess this is worth it
		// TODO(reed): there's a bug here where the between writers could have
		// bytes in there, get swapped for real stdout/stderr, come back and write
		// bytes in and the bytes are [really] stale. I played with fixing this
		// and mostly came to the conclusion that life is meaningless.
		buf1 := bufPool.Get().(*bytes.Buffer)
		buf2 := bufPool.Get().(*bytes.Buffer)
		bufs = []*bytes.Buffer{buf1, buf2}

		soc := &nopCloser{&logWriter{
			logrus.WithFields(logrus.Fields{"tag": "stdout", "app_id": call.AppID, "fn_id": call.FnID, "image": call.Image, "container_id": id}),
		}}
		sec := &nopCloser{&logWriter{
			logrus.WithFields(logrus.Fields{"tag": "stderr", "app_id": call.AppID, "fn_id": call.FnID, "image": call.Image, "container_id": id}),
		}}

		stdout.Swap(newLineWriterWithBuffer(buf1, soc))
		stderr.Swap(newLineWriterWithBuffer(buf2, sec))
	}

	var iofs iofs
	var err error
	if call.Format == models.FormatHTTPStream {
		// XXX(reed): we should also point stdout to stderr, and not have stdin
		if cfg.IOFSEnableTmpfs {
			iofs, err = newTmpfsIOFS(ctx, cfg)
		} else {
			iofs, err = newDirectoryIOFS(ctx, cfg)
		}

		if err != nil {
			return nil, err
		}
	} else {
		iofs = &noopIOFS{}
	}

	return &container{
		id:         id, // XXX we could just let docker generate ids...
		image:      call.Image,
		env:        map[string]string(call.Config),
		extensions: call.extensions,
		memory:     call.Memory,
		cpus:       uint64(call.CPUs),
		fsSize:     cfg.MaxFsSize,
		tmpFsSize:  uint64(call.TmpFsSize),
		iofs:       iofs,
		logCfg: drivers.LoggerConfig{
			URL: strings.TrimSpace(call.SyslogURL),
			Tags: []drivers.LoggerTag{
				{Name: "app_id", Value: call.AppID},
				{Name: "fn_id", Value: call.FnID},
			},
		},
		stdin:  stdin,
		stdout: stdout,
		stderr: stderr,
		close: func() {
			stdin.Close()
			stderr.Close()
			stdout.Close()
			for _, b := range bufs {
				bufPool.Put(b)
			}
			// iofs.Close MUST be called here or we will leak directories and/or tmpfs mounts!
			if err = iofs.Close(); err != nil {
				// Note: This is logged with the context of the container creation
				common.Logger(ctx).WithError(err).Error("Error closing IOFS")
			}
		},
	}, nil
}

func (c *container) swap(stdin io.Reader, stdout, stderr io.Writer, cs *drivers.Stats) func() {
	// if tests don't catch this, then fuck me
	ostdin := c.stdin.(common.GhostReader).Swap(stdin)
	ostdout := c.stdout.(common.GhostWriter).Swap(stdout)
	ostderr := c.stderr.(common.GhostWriter).Swap(stderr)

	c.swapMu.Lock()
	ocs := c.stats
	c.stats = cs
	c.swapMu.Unlock()

	return func() {
		c.stdin.(common.GhostReader).Swap(ostdin)
		c.stdout.(common.GhostWriter).Swap(ostdout)
		c.stderr.(common.GhostWriter).Swap(ostderr)
		c.swapMu.Lock()
		c.stats = ocs
		c.swapMu.Unlock()
	}
}

func (c *container) Id() string                         { return c.id }
func (c *container) Command() string                    { return "" }
func (c *container) Input() io.Reader                   { return c.stdin }
func (c *container) Logger() (io.Writer, io.Writer)     { return c.stdout, c.stderr }
func (c *container) Volumes() [][2]string               { return nil }
func (c *container) WorkDir() string                    { return "" }
func (c *container) Close()                             { c.close() }
func (c *container) Image() string                      { return c.image }
func (c *container) Timeout() time.Duration             { return c.timeout }
func (c *container) EnvVars() map[string]string         { return c.env }
func (c *container) Memory() uint64                     { return c.memory * 1024 * 1024 } // convert MB
func (c *container) CPUs() uint64                       { return c.cpus }
func (c *container) FsSize() uint64                     { return c.fsSize }
func (c *container) TmpFsSize() uint64                  { return c.tmpFsSize }
func (c *container) Extensions() map[string]string      { return c.extensions }
func (c *container) LoggerConfig() drivers.LoggerConfig { return c.logCfg }
func (c *container) UDSAgentPath() string               { return c.iofs.AgentPath() }
func (c *container) UDSDockerPath() string              { return c.iofs.DockerPath() }
func (c *container) UDSDockerDest() string              { return iofsDockerMountDest }

// WriteStat publishes each metric in the specified Stats structure as a histogram metric
func (c *container) WriteStat(ctx context.Context, stat drivers.Stat) {
	for key, value := range stat.Metrics {
		if m, ok := dockerMeasures[key]; ok {
			stats.Record(ctx, m.M(int64(value)))
		}
	}

	c.swapMu.Lock()
	if c.stats != nil {
		*(c.stats) = append(*(c.stats), stat)
	}
	c.swapMu.Unlock()
}

// DockerAuth implements the docker.AuthConfiguration interface.
func (c *container) DockerAuth() (*docker.AuthConfiguration, error) {
	registryToken := c.extensions[RegistryToken]
	if registryToken != "" {
		return &docker.AuthConfiguration{
			RegistryToken: registryToken,
		}, nil
	}
	return nil, nil
}
