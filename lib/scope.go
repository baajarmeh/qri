package lib

import (
	"context"

	"github.com/qri-io/dataset"
	"github.com/qri-io/qfs/muxfs"
	"github.com/qri-io/qri/config"
	"github.com/qri-io/qri/dscache"
	"github.com/qri-io/qri/dsref"
	"github.com/qri-io/qri/event"
	"github.com/qri-io/qri/fsi"
	"github.com/qri-io/qri/logbook"
	"github.com/qri-io/qri/profile"
	"github.com/qri-io/qri/repo"
	"github.com/qri-io/qri/stats"
)

// scope represents the lifetime of a method call, abstractly connected to the caller of
// that method, such that the implementation is unaware of how it has been invoked.
// Using scope instead of the global data on Instance lets us control user identity,
// permissions, and configuration, while also setting us up to properly run multiple
// operations at the same time to support multi-tenancy and multi-processing.
type scope struct {
	ctx  context.Context
	inst *Instance
	pro  *profile.Profile
	// TODO(dustmop): Additional information, such as user identity, their profile, keys
}

func newScope(ctx context.Context, inst *Instance) (scope, error) {
	pro, err := inst.activeProfile(ctx)
	if err != nil {
		return scope{}, err
	}

	return scope{
		ctx:  ctx,
		inst: inst,
		pro:  pro,
	}, nil
}

func (s *scope) ActiveProfile() *profile.Profile {
	return s.pro
}

// Context returns the context for this scope. Though this pattern is usually discouraged,
// we're following http.Request's lead, as scope plays the same role. The lifetime of a
// single scope matches the lifetime of the Context; this ownership is not long-lived
func (s *scope) Context() context.Context {
	return s.ctx
}

// AppContext returns the context of the top-level application, to enable graceful shutdowns
func (s *scope) AppContext() context.Context {
	return s.inst.appCtx
}

// FSISubsystem returns a reference to the FSI subsystem
// TODO(dustmop): This subsystem contains global data, we should move that data out and
// into scope
func (s *scope) FSISubsystem() *fsi.FSI {
	return s.inst.fsi
}

// Bus returns the event bus
func (s *scope) Bus() event.Bus {
	// TODO(dustmop): Filter only events for this scope.
	return s.inst.bus
}

// Filesystem returns a filesystem
func (s *scope) Filesystem() *muxfs.Mux {
	return s.inst.qfs
}

// Config returns the config
func (s *scope) Config() *config.Config {
	return s.inst.cfg
}

// Dscache returns the dscache
func (s *scope) Dscache() *dscache.Dscache {
	return s.inst.Dscache()
}

// Profiles accesses the profile store
func (s *scope) Profiles() profile.Store {
	return s.inst.profiles
}

// Repo returns the repo store
func (s *scope) Repo() repo.Repo {
	return s.inst.repo
}

// Logbook returns the repo logbook
func (s *scope) Logbook() *logbook.Book {
	return s.inst.logbook
}

// Loader returns the instance
func (s *scope) Loader() *Instance {
	return s.inst
}

// Stats returns the stats service
func (s *scope) Stats() *stats.Service {
	return s.inst.stats
}

// ParseAndResolveRef parses a reference and resolves it
func (s *scope) ParseAndResolveRef(ctx context.Context, refStr, source string) (dsref.Ref, string, error) {
	return s.inst.ParseAndResolveRef(ctx, refStr, source)
}

// ParseAndResolveRefWithWorkingDir parses a reference and resolves it with FSI info attached
func (s *scope) ParseAndResolveRefWithWorkingDir(ctx context.Context, refstr, source string) (dsref.Ref, string, error) {
	return s.inst.ParseAndResolveRefWithWorkingDir(ctx, refstr, source)
}

// LoadDataset loads a dataset
func (s *scope) LoadDataset(ctx context.Context, ref dsref.Ref, source string) (*dataset.Dataset, error) {
	return s.inst.LoadDataset(ctx, ref, source)
}

// ParseResolveFunc returns a function that can parse a ref, then resolve and load it
func (s *scope) ParseResolveFunc() dsref.ParseResolveLoad {
	return NewParseResolveLoadFunc("", s.inst.defaultResolver(), s.inst)
}

// GetVersionInfoShim is in the process of being removed. Try not to add new callers.
func (s *scope) GetVersionInfoShim(ref dsref.Ref) (*dsref.VersionInfo, error) {
	r := s.inst.Repo()
	return repo.GetVersionInfoShim(r, ref)
}
