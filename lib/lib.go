// Package lib implements core qri business logic. It exports
// canonical methods that a qri instance can perform regardless of
// client interface. API's of any sort must use lib methods
package lib

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	golog "github.com/ipfs/go-log"
	homedir "github.com/mitchellh/go-homedir"
	ma "github.com/multiformats/go-multiaddr"
	manet "github.com/multiformats/go-multiaddr/net"
	"github.com/qri-io/ioes"
	"github.com/qri-io/qfs"
	"github.com/qri-io/qfs/muxfs"
	"github.com/qri-io/qfs/qipfs"
	"github.com/qri-io/qri/auth/key"
	"github.com/qri-io/qri/auth/token"
	"github.com/qri-io/qri/base/dsfs"
	"github.com/qri-io/qri/config"
	"github.com/qri-io/qri/config/migrate"
	"github.com/qri-io/qri/dscache"
	"github.com/qri-io/qri/dsref"
	qrierr "github.com/qri-io/qri/errors"
	"github.com/qri-io/qri/event"
	"github.com/qri-io/qri/fsi"
	"github.com/qri-io/qri/fsi/hiddenfile"
	"github.com/qri-io/qri/fsi/watchfs"
	"github.com/qri-io/qri/logbook"
	"github.com/qri-io/qri/p2p"
	"github.com/qri-io/qri/profile"
	"github.com/qri-io/qri/registry/regclient"
	"github.com/qri-io/qri/remote"
	"github.com/qri-io/qri/repo"
	"github.com/qri-io/qri/repo/buildrepo"
	"github.com/qri-io/qri/stats"
)

var (
	// ErrBadArgs is an error for when a user provides bad arguments
	ErrBadArgs = errors.New("bad arguments provided")
	// ErrNoRepo is an error for  when a repo does not exist at a given path
	ErrNoRepo = errors.New("no repo exists")

	log = golog.Logger("lib")
)

// Methods is a related set of library functions
type Methods interface {
	// CoreRequestsName confirms participation in the CoreRequests interface while
	// also giving a human readable string for logging purposes
	// TODO (b5): rename this interface to "MethodsName", or remove entirely
	CoreRequestsName() string
}

// InstanceOptions provides details to NewInstance.
// New will alter InstanceOptions by applying
// any provided Option functions
// to distinguish "Options" from "Config":
// * Options contains state that can only be determined at runtime
// * Config consists only of static values stored in a configuration file
// Options may override config in specific cases to avoid undefined state
type InstanceOptions struct {
	Cfg     *config.Config
	Streams ioes.IOStreams

	statsCache stats.Cache
	node       *p2p.QriNode
	repo       repo.Repo
	qfs        *muxfs.Mux
	dscache    *dscache.Dscache
	regclient  *regclient.Client
	logbook    *logbook.Book
	profiles   profile.Store
	bus        event.Bus
	logAll     bool

	remoteMockClient bool
	// use OptRemoteOptions to set this
	remoteOptsFuncs []remote.OptionsFunc

	eventHandler event.Handler
	events       []event.Type
}

// InstanceContextKey is used by context to set keys for constucting a lib.Instance
type InstanceContextKey string

// Option is a function that manipulates config details when fed to New(). Fields on
// the o parameter may be null, functions cannot assume the Config is non-null.
type Option func(o *InstanceOptions) error

// OptConfig supplies a configuration directly
func OptConfig(cfg *config.Config) Option {
	return func(o *InstanceOptions) error {
		o.Cfg = cfg
		return nil
	}
}

// OptSetIPFSPath sets the directory to read IPFS from.
// Passing the empty string adjusts qri to use the go-ipfs default:
// first checking the IPFS_PATH env variable, then falling back to $HOME/.ipfs
// if no ipfs filesystem is configured, this option creates one
func OptSetIPFSPath(path string) Option {
	return func(o *InstanceOptions) error {
		if o.Cfg == nil {
			return fmt.Errorf("config is nil, can't set IPFS path")
		}
		if o.Cfg.Filesystems == nil {
			return fmt.Errorf("config Filesystems field is nil, can't set IPFS path")
		}

		if path == "" {
			path = os.Getenv("IPFS_PATH")
			if path == "" {
				dir, err := homedir.Dir()
				if err != nil {
					return err
				}
				path = filepath.Join(dir, ".ipfs")
			}
		}

		for i, fsc := range o.Cfg.Filesystems {
			if fsc.Type == "ipfs" {
				fsConfig := o.Cfg.Filesystems[i]
				if fsConfig.Config == nil {
					fsConfig.Config = map[string]interface{}{}
				}
				fsConfig.Config["path"] = path
				return nil
			}
		}

		o.Cfg.Filesystems = append([]qfs.Config{
			{
				Type: "ipfs",
				Config: map[string]interface{}{
					"path": path,
				},
			},
		}, o.Cfg.Filesystems...)

		return nil
	}
}

// OptIOStreams sets the input IOStreams
func OptIOStreams(streams ioes.IOStreams) Option {
	return func(o *InstanceOptions) error {
		o.Streams = streams
		return nil
	}
}

// OptStdIOStreams sets treams to std, stdout, & stderr
func OptStdIOStreams() Option {
	return func(o *InstanceOptions) error {
		o.Streams = ioes.NewStdIOStreams()
		return nil
	}
}

// OptSetOpenFileTimeout sets a timeout duration for opening files
func OptSetOpenFileTimeout(d time.Duration) Option {
	return func(_ *InstanceOptions) error {
		dsfs.OpenFileTimeoutDuration = d
		return nil
	}
}

// OptCheckConfigMigrations checks for any configuration migrations that may
// need to be run. running & updating config if so
func OptCheckConfigMigrations(shouldRunFn func() bool, errOnSuccess bool) Option {
	return func(o *InstanceOptions) error {
		if o.Cfg == nil {
			return fmt.Errorf("no config file to check for migrations")
		}

		err := migrate.RunMigrations(o.Streams, o.Cfg, shouldRunFn, errOnSuccess)
		if err != nil {
			return err
		}

		return nil
	}
}

// OptNoBootstrap ensures the node will not attempt to bootstrap to any other nodes
// in the network
func OptNoBootstrap() Option {
	return func(o *InstanceOptions) error {
		// ensure qri p2p bootstrap addresses are empty
		o.Cfg.P2P.BootstrapAddrs = []string{}
		// if we have a qipfs config, pass the `disableBootstrap` flag
		for _, qfsCfg := range o.Cfg.Filesystems {
			if qfsCfg.Type == qipfs.FilestoreType {
				qfsCfg.Config["disableBootstrap"] = true
			}
		}
		return nil
	}
}

// OptSetLogAll sets the logAll value so that debug level logging is enabled for all qri packages
func OptSetLogAll(logAll bool) Option {
	return func(o *InstanceOptions) error {
		o.logAll = logAll
		return nil
	}
}

// OptRemoteOptions provides options to the instance remote
// the provided configuration function is called with the Qri configuration-derived
// remote settings applied, allowing partial-overrides.
func OptRemoteOptions(fns []remote.OptionsFunc) Option {
	return func(o *InstanceOptions) error {
		o.remoteOptsFuncs = fns
		return nil
	}
}

// OptEnableRemote enables the remote functionality in the node
func OptEnableRemote() Option {
	return func(o *InstanceOptions) error {
		o.Cfg.Remote.Enabled = true
		return nil
	}
}

// OptQriNode configures bring-your-own qri node
func OptQriNode(node *p2p.QriNode) Option {
	return func(o *InstanceOptions) error {
		o.node = node
		if o.node.Repo != nil && o.repo == nil {
			o.repo = o.node.Repo
		}
		if o.node.Repo.Filesystem() != nil {
			o.qfs = o.node.Repo.Filesystem()
		}
		return nil
	}
}

// OptRegistryClient overrides any configured registry client
func OptRegistryClient(cli *regclient.Client) Option {
	return func(o *InstanceOptions) error {
		o.regclient = cli
		return nil
	}
}

// OptStatsCache overrides the configured stats cache
func OptStatsCache(statsCache stats.Cache) Option {
	return func(o *InstanceOptions) error {
		o.statsCache = statsCache
		return nil
	}
}

// OptLogbook overrides the configured logbook with a manually provided one
func OptLogbook(bk *logbook.Book) Option {
	return func(o *InstanceOptions) error {
		o.logbook = bk
		return nil
	}
}

// OptEventHandler provides an event handler & list of event types to subscribe
// to. The canonical list of events a qri instance emits are defined in the
// github.com/qri-io/qri/event package
// plase note that event handlers in qri are synchronous. A handler function
// that takes a long time to return will slow down the performance of qri
// generally
func OptEventHandler(handler event.Handler, events ...event.Type) Option {
	return func(o *InstanceOptions) error {
		o.eventHandler = handler
		o.events = events
		return nil
	}
}

// OptProfiles supplies a profile store for the instance
func OptProfiles(pros profile.Store) Option {
	return func(o *InstanceOptions) error {
		o.profiles = pros
		return nil
	}
}

// OptBus overrides the configured `event.Bus` with a manually provided one
func OptBus(bus event.Bus) Option {
	return func(o *InstanceOptions) error {
		o.bus = bus
		return nil
	}
}

// OptDscache overrides the configured `dscache.Dscache` with a manually provided one
func OptDscache(dscache *dscache.Dscache) Option {
	return func(o *InstanceOptions) error {
		o.dscache = dscache
		return nil
	}
}

// NewInstance creates a new Qri Instance, if no Option funcs are provided,
// New uses a default set of Option funcs. Any Option functions passed to this
// function must check whether their fields are nil or not.
func NewInstance(ctx context.Context, repoPath string, opts ...Option) (qri *Instance, err error) {
	log.Debugf("NewInstance repoPath=%s opts=%v", repoPath, opts)
	ctx, cancel := context.WithCancel(ctx)
	ok := false
	defer func() {
		if !ok {
			cancel()
		}
	}()

	if repoPath == "" {
		return nil, fmt.Errorf("repo path is required")
	}

	o := &InstanceOptions{}

	// attempt to load a base configuration from repoPath
	needsMigration := false
	if o.Cfg, err = loadRepoConfig(repoPath); err != nil {
		log.Debugf("loading config: %s", err)
		if o.Cfg != nil && o.Cfg.Revision != config.CurrentConfigRevision {
			log.Debugf("config requires a migration from revision %d to %d", o.Cfg.Revision, config.CurrentConfigRevision)
			needsMigration = true
		}
		if !needsMigration {
			return
		}
	}

	if len(opts) == 0 {
		// default to a standard composition of Option funcs
		opts = []Option{
			OptStdIOStreams(),
			OptCheckConfigMigrations(func() bool { return true }, false),
		}
	}
	for _, opt := range opts {
		if err = opt(o); err != nil {
			return nil, err
		}
	}

	if needsMigration {
		if o.Cfg, err = loadRepoConfig(repoPath); err != nil {
			log.Error("loading config: %s", err)
			return
		}
	}

	cfg := o.Cfg
	if cfg == nil {
		// If at this point we don't have a configuration pointer one couldn't be
		// loaded from repoPath, and a configuration wasn't provided through Options,
		// so qri needs to be set up
		err = fmt.Errorf("no qri repo found, please run `qri setup`")
		return
	} else if err = cfg.Validate(); err != nil {
		return
	}

	// If configuration does not have a path assigned, but the repo has a path and
	// is stored on the filesystem, add that path to the configuration.
	if cfg.Repo.Type == "fs" && cfg.Path() == "" {
		cfg.SetPath(filepath.Join(repoPath, "config.yaml"))
	}

	inst := &Instance{
		cancel: cancel,
		doneCh: make(chan struct{}),

		repoPath: repoPath,
		cfg:      cfg,

		qfs:      o.qfs,
		repo:     o.repo,
		node:     o.node,
		streams:  o.Streams,
		registry: o.regclient,
		logbook:  o.logbook,
		dscache:  o.dscache,
		profiles: o.profiles,
		bus:      o.bus,
		appCtx:   ctx,
	}
	qri = inst

	// configure logging straight away
	if cfg != nil && cfg.Logging != nil {
		for name, level := range cfg.Logging.Levels {
			golog.SetLogLevel(name, level)
		}
	}

	// if logAll is enabled, turn on debug level logging for all qri packages. Packages need to
	// be explicitly enumerated here
	if o.logAll {
		allPackages := []string{"qriapi", "qrip2p", "base", "changes", "cmd", "config", "dsref", "dsfs", "friendly", "fsi", "lib", "logbook", "profile", "repo", "sql"}
		for _, name := range allPackages {
			golog.SetLogLevel(name, "debug")
		}
		log.Debugf("--log-all set: turning on logging for all activity")
	}

	inst.RegisterMethods()

	// check if we're operating over RPC
	if cfg.RPC.Enabled {
		addr, err := ma.NewMultiaddr(cfg.API.Address)
		if err != nil {
			return nil, qrierr.New(err, fmt.Sprintf("invalid config.api.address value: %q", cfg.API.Address))
		}
		_, err = manet.Dial(addr)
		if err == nil {
			// we have a connection
			inst.http, err = NewHTTPClient(cfg.API.Address)
			if err != nil {
				return nil, err
			}

			go inst.waitForAllDone()
			return qri, err
		}
	}

	if inst.bus == nil {
		inst.bus = newEventBus(ctx)
	}

	if o.eventHandler != nil && o.events != nil {
		inst.bus.SubscribeTypes(o.eventHandler, o.events...)
	}

	if inst.qfs == nil {
		inst.qfs, err = buildrepo.NewFilesystem(ctx, cfg)
		if err != nil {
			return nil, err
		}

		go func() {
			inst.releasers.Add(1)
			<-inst.qfs.Done()
			inst.doneErr = inst.qfs.DoneErr()
			inst.releasers.Done()
		}()
	}

	if inst.keystore == nil {
		inst.keystore, err = key.NewStore(cfg)
		if err != nil {
			log.Debugw("initializing keystore", "err", err)
			return nil, err
		}
	}

	if inst.profiles == nil {
		if inst.profiles, err = profile.NewStore(cfg, inst.keystore); err != nil {
			return nil, fmt.Errorf("initializing profile service: %w", err)
		}
	}

	pro := inst.profiles.Owner()

	if inst.logbook == nil {
		inst.logbook, err = newLogbook(inst.qfs, cfg, inst.bus, pro, inst.repoPath)
		if err != nil {
			return nil, fmt.Errorf("intializing logbook: %w", err)
		}
	}

	if inst.registry == nil {
		inst.registry = newRegClient(ctx, cfg)
	}

	if inst.dscache == nil {
		inst.dscache, err = newDscache(ctx, inst.qfs, inst.bus, pro.Peername, inst.repoPath)
		if err != nil {
			log.Error("initalizing dscache:", err.Error())
			return nil, fmt.Errorf("newDsache: %w", err)
		}
	}

	if inst.repo == nil {
		if inst.repo, err = buildrepo.New(ctx, inst.repoPath, cfg, func(o *buildrepo.Options) {
			o.Bus = inst.bus
			o.Filesystem = inst.qfs
			o.Profiles = inst.profiles
			o.Logbook = inst.logbook
			o.Dscache = inst.dscache
			o.Keystore = inst.keystore
		}); err != nil {
			log.Error("intializing repo:", err.Error())
			return nil, fmt.Errorf("newRepo: %w", err)
		}
	}

	// Try to make the repo a hidden directory, but it's okay if we can't. Ignore the error.
	_ = hiddenfile.SetFileHidden(inst.repoPath)
	inst.fsi = fsi.NewFSI(inst.repo, inst.bus)

	if o.statsCache != nil {
		inst.stats = stats.New(o.statsCache)
	} else if inst.stats == nil {
		if inst.stats, err = newStats(cfg, inst.repoPath); err != nil {
			return nil, err
		}
	}

	if inst.node == nil {
		var localResolver dsref.Resolver
		localResolver, err = inst.resolverForMode("local")
		if err != nil {
			return
		}
		if inst.node, err = p2p.NewQriNode(inst.repo, cfg.P2P, inst.bus, localResolver); err != nil {
			log.Error("intializing p2p:", err.Error())
			return
		}
	}

	// Check if this is coming from a test, which is requesting a MockRemoteClient.
	key := InstanceContextKey("RemoteClient")
	if v := ctx.Value(key); v != nil && v == "mock" && inst.node != nil {
		inst.node.LocalStreams = inst.streams
		if inst.remoteClient, err = remote.NewMockClient(ctx, inst.node, inst.logbook); err != nil {
			return
		}
		go func() {
			inst.releasers.Add(1)
			<-inst.remoteClient.Done()
			inst.releasers.Done()
		}()

	} else if inst.node != nil {
		inst.node.LocalStreams = inst.streams

		if _, e := inst.node.IPFSCoreAPI(); e == nil {
			if inst.remoteClient, err = remote.NewClient(ctx, inst.node, inst.bus); err != nil {
				log.Error("initializing remote client:", err.Error())
				return
			}
			go func() {
				inst.releasers.Add(1)
				<-inst.remoteClient.Done()
				inst.releasers.Done()
			}()
		}

		if cfg.Remote != nil && cfg.Remote.Enabled {
			if o.remoteOptsFuncs == nil {
				o.remoteOptsFuncs = []remote.OptionsFunc{}
			}

			localResolver, resolverErr := inst.resolverForMode("local")
			if resolverErr != nil {
				return nil, resolverErr
			}

			if inst.remote, err = remote.NewRemote(inst.node, cfg.Remote, localResolver, o.remoteOptsFuncs...); err != nil {
				log.Error("intializing remote:", err.Error())
				return
			}
			// TODO (ramfox): we need to preserve these options
			// for if we need to re initalize the remote & don't have access
			// to those options again (this happens in the `GoOnline` func below)
			inst.remoteOptsFuncs = o.remoteOptsFuncs
		}
	}

	go inst.waitForAllDone()
	go func() {
		if err := inst.bus.Publish(ctx, event.ETInstanceConstructed, nil); err != nil {
			log.Debugf("instance construction: %w", err)
			err = nil
		}
	}()

	ok = true
	return
}

// TODO (b5): this is a repo layout assertion, move to repo package?
func loadRepoConfig(repoPath string) (*config.Config, error) {
	path := filepath.Join(repoPath, "config.yaml")

	if _, e := os.Stat(path); os.IsNotExist(e) {
		return nil, nil
	}

	return config.ReadFromFile(path)
}

func newRegClient(ctx context.Context, cfg *config.Config) (rc *regclient.Client) {
	if cfg.Registry != nil {
		switch cfg.Registry.Location {
		case "":
			return rc
		default:
			return regclient.NewClient(&regclient.Config{
				Location: cfg.Registry.Location,
			})
		}
	}

	return nil
}

func newLogbook(fs qfs.Filesystem, cfg *config.Config, bus event.Bus, pro *profile.Profile, repoPath string) (book *logbook.Book, err error) {
	logbookPath := filepath.Join(repoPath, "logbook.qfb")
	return logbook.NewJournal(pro.PrivKey, pro.Peername, bus, fs, logbookPath)
}

func newDscache(ctx context.Context, fs qfs.Filesystem, bus event.Bus, username, repoPath string) (*dscache.Dscache, error) {
	dscachePath := filepath.Join(repoPath, "dscache.qfb")
	return dscache.NewDscache(ctx, fs, bus, username, dscachePath), nil
}

func newEventBus(ctx context.Context) event.Bus {
	return event.NewBus(ctx)
}

func newStats(cfg *config.Config, repoPath string) (*stats.Service, error) {
	// The stats cache default location is repoPath/stats
	// can be overridden in the config: cfg.Stats.Path
	path := filepath.Join(repoPath, "stats")
	if cfg.Stats == nil {
		return stats.New(nil), nil
	}
	if cfg.Stats.Cache.Path != "" {
		path = cfg.Stats.Cache.Path
	}
	switch cfg.Stats.Cache.Type {
	case "fs", "local":
		cache, err := stats.NewLocalCache(path, int64(cfg.Stats.Cache.MaxSize))
		if err != nil {
			return nil, err
		}
		return stats.New(cache), nil
	default:
		return stats.New(nil), nil
	}
}

// NewInstanceFromConfigAndNode is a temporary solution to create an instance from an
// already-allocated QriNode & configuration
// don't write new code that relies on this, instead create a configuration
// and options that can be fed to NewInstance
// This function must only be used for testing purposes
func NewInstanceFromConfigAndNode(ctx context.Context, cfg *config.Config, node *p2p.QriNode) *Instance {
	return NewInstanceFromConfigAndNodeAndBus(ctx, cfg, node, event.NilBus)
}

// NewInstanceFromConfigAndNodeAndBus adds a bus argument to the horrible, hacky
// instance construtor
func NewInstanceFromConfigAndNodeAndBus(ctx context.Context, cfg *config.Config, node *p2p.QriNode, bus event.Bus) *Instance {
	ctx, cancel := context.WithCancel(ctx)

	r := node.Repo
	pro := r.Profiles().Owner()
	fsint := fsi.NewFSI(r, bus)
	dc := dscache.NewDscache(ctx, r.Filesystem(), bus, pro.Peername, "")

	// TODO (b5) - lots of tests pass "DefaultConfigForTesting", which uses a different peername /
	// identity from what the repo already has. This disagreement is a potential source of bugs
	// we should fix this by getting over to lib.NewInstance ASAP
	if cfg.Profile.Peername != pro.Peername {
		cfg.Profile.Peername = pro.Peername
	}

	inst := &Instance{
		cancel: cancel,
		doneCh: make(chan struct{}),

		cfg:      cfg,
		node:     node,
		dscache:  dc,
		logbook:  r.Logbook(),
		profiles: r.Profiles(),
		appCtx:   ctx,
	}
	inst.RegisterMethods()

	inst.stats = stats.New(nil)

	if node != nil && r != nil {
		inst.repo = r
		inst.bus = bus
		inst.fsi = fsint
		inst.qfs = r.Filesystem()
	}

	var err error
	inst.remoteClient, err = remote.NewClient(ctx, node, inst.bus)
	if err != nil {
		cancel()
		panic(err)
	}
	go func() {
		inst.releasers.Add(1)
		<-inst.remoteClient.Done()
		inst.releasers.Done()
	}()

	go inst.waitForAllDone()
	return inst
}

// Instance bundles the foundational values qri relies on, including a qri
// configuration, p2p node, and base context.
// An instance wraps required state for for "Method" constructors, which
// contain qri business logic. Think of instance as the "core" of the qri
// ecosystem. Create an Instance pointer with NewInstance
type Instance struct {
	repoPath string
	cfg      *config.Config

	regMethods *regMethodSet

	streams      ioes.IOStreams
	repo         repo.Repo
	node         *p2p.QriNode
	qfs          *muxfs.Mux
	fsi          *fsi.FSI
	remote       *remote.Remote
	remoteClient remote.Client
	registry     *regclient.Client
	stats        *stats.Service
	logbook      *logbook.Book
	dscache      *dscache.Dscache
	bus          event.Bus
	watcher      *watchfs.FilesysWatcher
	appCtx       context.Context

	profiles profile.Store
	keystore key.Store

	remoteOptsFuncs []remote.OptionsFunc

	http *HTTPClient

	cancel    context.CancelFunc
	doneCh    chan struct{}
	doneErr   error
	releasers sync.WaitGroup
}

// Connect takes an instance online
func (inst *Instance) Connect(ctx context.Context) (err error) {
	if err = inst.node.GoOnline(ctx); err != nil {
		log.Debugf("taking node online: %s", err.Error())
		return
	}

	// for now if we have an IPFS node instance, node.GoOnline has to make a new
	// instance to connect properly. If remoteClient or remote retains the reference to the
	// old instance, we run into issues where the online instance can't "see"
	// the additions. We fix that by shutting down the previous client and
	// re-initializing the client and remote with the new instance
	if inst.remoteClient != nil {
		<-inst.remoteClient.Shutdown()
	}
	// NOTE: the previous remote client got its context from the context that is
	// tied to the life of the instance. This one is tied to the life of the
	// `Connect` function. The instance is responsible for cleaning up the
	// remoteClient, since it cannot rely on this context to cancel at the same
	// time as the context of the instance does
	if inst.remoteClient, err = remote.NewClient(ctx, inst.node, inst.bus); err != nil {
		log.Debugf("remote.NewClient error=%q", err)
		return
	}
	go func() {
		inst.releasers.Add(1)
		<-inst.remoteClient.Done()
		inst.releasers.Done()
	}()

	if inst.cfg.Remote != nil && inst.cfg.Remote.Enabled {
		localResolver, err := inst.resolverForMode("local")
		if err != nil {
			return err
		}
		if inst.remote, err = remote.NewRemote(inst.node, inst.cfg.Remote, localResolver, inst.remoteOptsFuncs...); err != nil {
			log.Debugf("remote.NewRemote error=%q", err)
			return err
		}
		if err = inst.remote.GoOnline(ctx); err != nil {
			log.Debugf("remote.GoOnline error=%q", err)
			return err
		}
	}

	return nil
}

// Access returns the AccessMethods that Instance has registered
func (inst *Instance) Access() AccessMethods {
	return AccessMethods{d: inst}
}

// Config returns the ConfigMethods that Instance has registered
func (inst *Instance) Config() ConfigMethods {
	return ConfigMethods{d: inst}
}

// Dataset returns the DatasetMethods that Instance has registered
func (inst *Instance) Dataset() DatasetMethods {
	return DatasetMethods{d: inst}
}

// Filesys returns the FSIMethods that Instance has registered
func (inst *Instance) Filesys() FSIMethods {
	return FSIMethods{d: inst}
}

// Log returns the LogMethods that Instance has registered
func (inst *Instance) Log() LogMethods {
	return LogMethods{d: inst}
}

// Peer returns the PeerMethods that Instance has registered
func (inst *Instance) Peer() PeerMethods {
	return PeerMethods{d: inst}
}

// Search returns the SearchMethods that Instance has registered
func (inst *Instance) Search() SearchMethods {
	return SearchMethods{d: inst}
}

// SQL returns the SQLMethods that Instance has registered
func (inst *Instance) SQL() SQLMethods {
	return SQLMethods{d: inst}
}

// Transform returns the TransformMethods that Instance has registered
func (inst *Instance) Transform() TransformMethods {
	return TransformMethods{d: inst}
}

// WithSource returns a wrapped instance that will resolve refs from the given source
func (inst *Instance) WithSource(source string) *InstanceSourceWrap {
	return &InstanceSourceWrap{
		source: source,
		inst:   inst,
	}
}

// InstanceSourceWrap is a wrapped instance with an explicit resolver source added
type InstanceSourceWrap struct {
	source string
	inst   *Instance
}

// Dataset returns the DatasetMethods that Instance has registered
func (isw *InstanceSourceWrap) Dataset() DatasetMethods {
	return DatasetMethods{d: isw}
}

// SQL returns the SQLMethods that Instance has registered
func (isw *InstanceSourceWrap) SQL() SQLMethods {
	return SQLMethods{d: isw}
}

// GetConfig provides methods for manipulating Qri configuration
//
// Deprecated: this method will be removed in a future release.
// Use inst.Config().GetConfig instead
func (inst *Instance) GetConfig() *config.Config {
	if inst == nil {
		return nil
	}
	return inst.cfg
}

// Shutdown closes the instance, releasing all held resources. the returned
// channel will write any closing error, including context cancellation
// timeout
func (inst *Instance) Shutdown() <-chan error {
	errCh := make(chan error)
	// NOTE: the remote client may have gotten its context from the `Connect` func
	// not the context that the instance itself was built around.
	// The instance must clean up the remoteClient, since it cannot rely on the
	// remote client's context to cancel at the same time as the instance's context
	if inst.remoteClient != nil {
		<-inst.remoteClient.Shutdown()
	}
	// NOTE: when the QriNode goes "Online" it creates a new context, like the
	// above remote client, we have to explicitly "GoOffline" in order to make
	// sure we are releasing all resources
	inst.node.GoOffline()
	go func() {
		<-inst.doneCh
		errCh <- inst.doneErr
	}()
	inst.cancel()
	return errCh
}

// FSI returns methods for using filesystem integration
func (inst *Instance) FSI() *fsi.FSI {
	if inst == nil {
		return nil
	}
	return inst.fsi
}

// ChangeConfig implements the ConfigSetter interface
func (inst *Instance) ChangeConfig(cfg *config.Config) (err error) {
	cfg = cfg.WithPrivateValues(inst.cfg)

	if path := inst.cfg.Path(); path != "" {
		if err = cfg.WriteToFile(path); err != nil {
			return
		}
	}

	inst.cfg = cfg
	return nil
}

// Node accesses the instance qri node if one exists
func (inst *Instance) Node() *p2p.QriNode {
	if inst == nil {
		return nil
	}
	return inst.node
}

// Repo accesses the instance Repo if one exists
func (inst *Instance) Repo() repo.Repo {
	if inst == nil {
		return nil
	}
	if inst.repo != nil {
		return inst.repo
	} else if inst.node != nil {
		return inst.node.Repo
	}
	return nil
}

// RepoPath returns the path to the directory qri is operating from
func (inst *Instance) RepoPath() string {
	if inst == nil {
		return ""
	}
	return inst.repoPath
}

// Dscache returns the dscache that the instance has
func (inst *Instance) Dscache() *dscache.Dscache {
	if inst == nil {
		return nil
	}
	return inst.dscache
}

// HTTPClient accesses the instance HTTP client if one exists
func (inst *Instance) HTTPClient() *HTTPClient {
	if inst == nil {
		return nil
	}
	return inst.http
}

// Remote accesses the remote subsystem if one exists
func (inst *Instance) Remote() *remote.Remote {
	if inst == nil {
		return nil
	}
	return inst.remote
}

// RemoteClient exposes the instance client for making requests to remotes
func (inst *Instance) RemoteClient() remote.Client {
	if inst == nil {
		return nil
	}
	return inst.remoteClient
}

// Bus exposes the instance event bus
func (inst *Instance) Bus() event.Bus {
	if inst == nil {
		return nil
	}
	return inst.bus
}

// activeProfile tries to extract the current user from values embedded in the
// passed-in context, falling back to the repo owner as a default active profile
func (inst *Instance) activeProfile(ctx context.Context) (pro *profile.Profile, err error) {
	if inst == nil {
		return nil, fmt.Errorf("no instance")
	}

	if tokenString := token.FromCtx(ctx); tokenString != "" {
		tok, err := token.ParseAuthToken(tokenString, inst.keystore)
		if err != nil {
			return nil, err
		}

		if claims, ok := tok.Claims.(*token.Claims); ok {
			// TODO(b5): at this point we have a valid signature of a profileID string
			// but no proof that this profile is owned by the key that signed the
			// token. We either need ProfileID == KeyID, or we need a UCAN. we need to
			// check for those, ideally in a method within the profile package that
			// abstracts over profile & key agreement
			pro, err := inst.profiles.GetProfile(profile.IDB58DecodeOrEmpty(claims.ProfileID))
			if errors.Is(err, profile.ErrNotFound) {
				return nil, fmt.Errorf("request profile not sent")
			}
			return pro, err
		}
	}

	if inst.profiles != nil {
		return inst.profiles.Owner(), nil
	}

	if pro == nil {
		return nil, fmt.Errorf("no active profile")
	}

	return pro, err
}

// checkRPCError validates RPC errors and in case of EOF returns a
// more user friendly message
func checkRPCError(err error) error {
	if err == nil {
		return nil
	}
	if strings.Contains(err.Error(), "EOF") {
		msg := `Qri couldn't parse the response and is unsure if it was successful. 
It is possible you have a Qri node running or the Desktop app is open.
Try closing them and running the command again.
Check our issue tracker for RPC issues & feature requests:
  https://github.com/qri-io/qri/issues?q=is:issue+label:RPC

Error:
%s`
		return qrierr.New(err, fmt.Sprintf(msg, err.Error()))
	}
	return err
}

func (inst *Instance) waitForAllDone() {
	inst.releasers.Wait()
	log.Debug("closing instance")
	close(inst.doneCh)
}
