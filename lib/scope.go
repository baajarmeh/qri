package lib

import (
	"context"

	"github.com/qri-io/qfs"
	"github.com/qri-io/qri/dscache"
	"github.com/qri-io/qri/dsref"
	"github.com/qri-io/qri/event"
	"github.com/qri-io/qri/fsi"
)

// Scope represents the lifetime of a method call, abstractly connected to the caller of that
// method, such that the implementation is unaware of how it has been invoked.
type Scope struct {
	ctx  context.Context
	inst *Instance
	// TODO: Additional information, including the user identity, their profile, keys
}

func NewScope(ctx context.Context, inst *Instance) *Scope {
	return &Scope{
		ctx:  ctx,
		inst: inst,
	}
}

func (s *Scope) Context() context.Context {
	return s.ctx
}

func (s *Scope) FSISubsystem() *fsi.FSI {
	return s.inst.fsi
}

func (s *Scope) Bus() event.Bus {
	// TODO: Filter only events for this scope.
	return s.inst.bus
}

func (s *Scope) Filesystem() qfs.Filesystem {
	return s.inst.qfs
}

func (s *Scope) Dscache() *dscache.Dscache {
	return s.inst.Dscache()
}

func (s *Scope) ParseAndResolveRef(ctx context.Context, refStr, source string) (dsref.Ref, string, error) {
	return s.inst.ParseAndResolveRef(ctx, refStr, source)
}

func (s *Scope) ParseAndResolveRefWithWorkingDir(ctx context.Context, refstr, source string) (dsref.Ref, string, error) {
	return s.inst.ParseAndResolveRefWithWorkingDir(ctx, refstr, source)
}

func (s *Scope) Loader() dsref.ParseResolveLoad {
	return NewParseResolveLoadFunc("", s.inst.defaultResolver(), s.inst)
}

