// Package api implements a JSON-API for interacting with a qri node
package api

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"time"

	"github.com/gorilla/mux"
	golog "github.com/ipfs/go-log"
	apiutil "github.com/qri-io/qri/api/util"
	"github.com/qri-io/qri/auth/token"
	"github.com/qri-io/qri/lib"
	"github.com/qri-io/qri/version"
)

var (
	log = golog.Logger("qriapi")
	// APIVersion is the version string that is written in API responses
	APIVersion = version.Version
)

const (
	// DefaultTemplateHash is the hash of the default render template
	DefaultTemplateHash = "/ipfs/QmeqeRTf2Cvkqdx4xUdWi1nJB2TgCyxmemsL3H4f1eTBaw"
	// TemplateUpdateAddress is the URI for the template update
	TemplateUpdateAddress = "/ipns/defaulttmpl.qri.io"
)

func init() {
	golog.SetLogLevel("qriapi", "info")
}

// Server wraps a qri p2p node, providing traditional access via http
// Create one with New, start it up with Serve
type Server struct {
	*lib.Instance
	Mux       *mux.Router
	websocket lib.WebsocketHandler
}

// New creates a new qri server from a p2p node & configuration
func New(inst *lib.Instance) Server {
	return Server{
		Instance: inst,
	}
}

// Serve starts the server. It will block while the server is running
func (s Server) Serve(ctx context.Context) (err error) {
	node := s.Node()
	cfg := s.GetConfig()

	ws, err := lib.NewWebsocketHandler(ctx, s.Instance)
	if err != nil {
		return err
	}
	s.websocket = ws
	s.Mux = NewServerRoutes(s)

	if err := s.Instance.Connect(ctx); err != nil {
		return err
	}

	server := &http.Server{
		Handler: s.Mux,
	}

	info := "\n📡  Success! You are now connected to the d.web. Here's your connection details:\n"
	info += cfg.SummaryString()
	info += "IPFS Addresses:"
	for _, a := range node.EncapsulatedAddresses() {
		info = fmt.Sprintf("%s\n  %s", info, a.String())
	}
	info += fmt.Sprintf("\nYou are running Qri v%s", APIVersion)
	info += "\n\n"

	node.LocalStreams.Print(info)

	if cfg.API.DisconnectAfter != 0 {
		log.Infof("disconnecting after %d seconds", cfg.API.DisconnectAfter)
		go func(s *http.Server, t int) {
			<-time.After(time.Second * time.Duration(t))
			log.Infof("disconnecting")
			s.Close()
		}(server, cfg.API.DisconnectAfter)
	}

	go func() {
		<-ctx.Done()
		log.Info("shutting down")
		server.Close()
	}()

	// http.ListenAndServe will not return unless there's an error
	return StartServer(cfg.API, server)
}

// HandleIPFSPath responds to IPFS Hash requests with raw data
func (s *Server) HandleIPFSPath(w http.ResponseWriter, r *http.Request) {
	if s.GetConfig().API.ReadOnly {
		readOnlyResponse(w, "/ipfs/")
		return
	}

	file, err := s.Node().Repo.Filesystem().Get(r.Context(), r.URL.Path)
	if err != nil {
		apiutil.WriteErrResponse(w, http.StatusInternalServerError, err)
		return
	}

	io.Copy(w, file)
}

// helper function
func readOnlyResponse(w http.ResponseWriter, endpoint string) {
	apiutil.WriteErrResponse(w, http.StatusForbidden, fmt.Errorf("qri server is in read-only mode, access to '%s' endpoint is forbidden", endpoint))
}

// HomeHandler responds with a health check on the empty path, 404 for
// everything else
func (s *Server) HomeHandler(w http.ResponseWriter, r *http.Request) {
	upgrade := r.Header.Get("Upgrade")
	if upgrade == "websocket" {
		s.websocket.WSConnectionHandler(w, r)
	} else {
		if r.URL.Path == "" || r.URL.Path == "/" {
			HealthCheckHandler(w, r)
			return
		}

		apiutil.NotFoundHandler(w, r)
	}
}

// HealthCheckHandler is a basic ok response for load balancers & co
// returns the version of qri this node is running, pulled from the lib package
func HealthCheckHandler(w http.ResponseWriter, r *http.Request) {
	w.WriteHeader(http.StatusOK)
	w.Write([]byte(`{ "meta": { "code": 200, "status": "ok", "version":"` + APIVersion + `" }, "data": [] }`))
}

// refRouteParams carry a config for a ref based route
type refRouteParams struct {
	Endpoint lib.APIEndpoint
	ShortRef bool
	Selector bool
	Methods  []string
}

// newrefRouteParams is a shorthand to generate refRouteParams
func newrefRouteParams(e lib.APIEndpoint, sr bool, sel bool, methods ...string) refRouteParams {
	return refRouteParams{
		Endpoint: e,
		ShortRef: sr,
		Selector: sel,
		Methods:  methods,
	}
}

func handleRefRoute(m *mux.Router, p refRouteParams, f http.HandlerFunc) {
	routes := []string{
		p.Endpoint.String(),
		fmt.Sprintf("%s/%s", p.Endpoint, "{peername}/{name}"),
	}
	if p.Selector {
		routes = append(routes, fmt.Sprintf("%s/%s", p.Endpoint, "{peername}/{name}/{selector}"))
	}
	if !p.ShortRef {
		routes = append(routes, fmt.Sprintf("%s/%s", p.Endpoint, "{peername}/{name}/at/{fs}/{hash}"))
		if p.Selector {
			routes = append(routes, fmt.Sprintf("%s/%s", p.Endpoint, "{peername}/{name}/at/{fs}/{hash}/{selector}"))
		}
	}

	if p.Methods == nil {
		p.Methods = []string{}
	}

	for _, route := range routes {
		if len(p.Methods) > 0 {
			hasOptions := false
			for _, o := range p.Methods {
				if o == http.MethodOptions {
					hasOptions = true
					break
				}
			}
			if !hasOptions {
				p.Methods = append(p.Methods, http.MethodOptions)
			}
			// TODO(b5): this is a band-aid that lets us punt on teaching lib about how to
			// switch on HTTP verbs. I think we should use tricks like this that leverage
			// the gorilla/mux package until we get a better sense of how our API uses
			// HTTP verbs
			m.Handle(route, f).Methods(p.Methods...)
		} else {
			m.Handle(route, f)
		}
	}
}

// NewServerRoutes returns a Muxer that has all API routes
func NewServerRoutes(s Server) *mux.Router {
	cfg := s.GetConfig()

	m := s.Mux
	if m == nil {
		m = mux.NewRouter()
	}
	m.Use(corsMiddleware(cfg.API.AllowedOrigins))
	m.Use(muxVarsToQueryParamMiddleware)
	m.Use(refStringMiddleware)
	m.Use(token.OAuthTokenMiddleware)

	var routeParams refRouteParams

	m.Handle(lib.AEHome.String(), s.NoLogMiddleware(s.HomeHandler))
	m.Handle(lib.AEHealth.String(), s.NoLogMiddleware(HealthCheckHandler))
	m.Handle(lib.AEIPFS.String(), s.Middleware(s.HandleIPFSPath))

	proh := NewProfileHandlers(s.Instance, cfg.API.ReadOnly)
	m.Handle(lib.AEMe.String(), s.Middleware(proh.ProfileHandler))
	m.Handle(lib.AEProfile.String(), s.Middleware(proh.ProfileHandler))
	m.Handle(lib.AEProfilePhoto.String(), s.Middleware(proh.ProfilePhotoHandler))
	m.Handle(lib.AEProfilePoster.String(), s.Middleware(proh.PosterHandler))

	m.Handle(lib.AEPeers.String(), s.Middleware(lib.NewHTTPRequestHandler(s.Instance, "peer.list"))).Methods(http.MethodPost)
	m.Handle(lib.AEPeer.String(), s.Middleware(lib.NewHTTPRequestHandler(s.Instance, "peer.info"))).Methods(http.MethodPost)
	m.Handle(lib.AEConnect.String(), s.Middleware(lib.NewHTTPRequestHandler(s.Instance, "peer.connect"))).Methods(http.MethodPost)
	m.Handle(lib.AEDisconnect.String(), s.Middleware(lib.NewHTTPRequestHandler(s.Instance, "peer.disconnect"))).Methods(http.MethodPost)
	m.Handle(lib.AEConnections.String(), s.Middleware(lib.NewHTTPRequestHandler(s.Instance, "peer.connections"))).Methods(http.MethodPost)
	m.Handle(lib.AEConnectedQriProfiles.String(), s.Middleware(lib.NewHTTPRequestHandler(s.Instance, "peer.connectedqriprofiles"))).Methods(http.MethodPost)

	if cfg.Remote != nil && cfg.Remote.Enabled {
		log.Info("running in `remote` mode")

		remh := NewRemoteHandlers(s.Instance)
		m.Handle(lib.AERemoteDSync.String(), s.Middleware(remh.DsyncHandler))
		m.Handle(lib.AERemoteLogSync.String(), s.Middleware(remh.LogsyncHandler))
		m.Handle(lib.AERemoteRefs.String(), s.Middleware(remh.RefsHandler))
	}

	dsh := NewDatasetHandlers(s.Instance, cfg.API.ReadOnly)
	m.Handle(lib.AEList.String(), s.Middleware(lib.NewHTTPRequestHandler(s.Instance, "dataset.list"))).Methods(http.MethodPost, http.MethodGet)
	m.Handle(lib.AEListRaw.String(), s.Middleware(lib.NewHTTPRequestHandler(s.Instance, "dataset.listrawrefs"))).Methods(http.MethodPost, http.MethodGet)
	m.Handle(lib.AEPeerList.String(), s.Middleware(dsh.PeerListHandler(lib.AEPeerList.String())))
	routeParams = newrefRouteParams(lib.AESave, false, false, http.MethodPost, http.MethodPut)
	handleRefRoute(m, routeParams, s.Middleware(dsh.SaveHandler(lib.AESave.String())))
	routeParams = newrefRouteParams(lib.AEGet, false, true, http.MethodGet, http.MethodPost)
	handleRefRoute(m, routeParams, s.Middleware(dsh.GetHandler(lib.AEGet.String())))
	routeParams = newrefRouteParams(lib.AERemove, false, false, http.MethodPost, http.MethodDelete)
	handleRefRoute(m, routeParams, s.Middleware(dsh.RemoveHandler(lib.AERemove.String())))
	m.Handle(lib.AERename.String(), s.Middleware(lib.NewHTTPRequestHandler(s.Instance, "dataset.rename"))).Methods(http.MethodPost, http.MethodPut)
	routeParams = newrefRouteParams(lib.AEValidate, false, false, http.MethodGet, http.MethodPost)
	handleRefRoute(m, routeParams, s.Middleware(lib.NewHTTPRequestHandler(s.Instance, "dataset.validate")))
	m.Handle(lib.AEDiff.String(), s.Middleware(lib.NewHTTPRequestHandler(s.Instance, "dataset.diff"))).Methods(http.MethodPost, http.MethodGet)
	m.Handle(lib.AEChanges.String(), s.Middleware(lib.NewHTTPRequestHandler(s.Instance, "dataset.changereport"))).Methods(http.MethodPost, http.MethodGet)
	m.Handle(lib.AEUnpack.String(), s.Middleware(dsh.UnpackHandler(lib.AEUnpack.NoTrailingSlash())))
	routeParams = newrefRouteParams(lib.AEManifest, false, false, http.MethodPost)
	handleRefRoute(m, routeParams, s.Middleware(lib.NewHTTPRequestHandler(s.Instance, "dataset.manifest")))
	m.Handle(lib.AEManifestMissing.String(), s.Middleware(lib.NewHTTPRequestHandler(s.Instance, "dataset.manifestmissing"))).Methods(http.MethodPost)
	routeParams = newrefRouteParams(lib.AEDAGInfo, false, false, http.MethodPost)
	handleRefRoute(m, routeParams, s.Middleware(lib.NewHTTPRequestHandler(s.Instance, "dataset.daginfo")))

	remClientH := NewRemoteClientHandlers(s.Instance, cfg.API.ReadOnly)
	routeParams = newrefRouteParams(lib.AEPush, false, false, http.MethodGet, http.MethodPost, http.MethodDelete)
	handleRefRoute(m, routeParams, s.Middleware(remClientH.PushHandler))
	routeParams = newrefRouteParams(lib.AEPull, false, false, http.MethodPost, http.MethodPut)
	handleRefRoute(m, routeParams, s.Middleware(dsh.PullHandler(lib.AEPull.NoTrailingSlash())))
	m.Handle(lib.AEFeeds.String(), s.Middleware(remClientH.FeedsHandler))
	routeParams = newrefRouteParams(lib.AEPreview, false, false, http.MethodGet, http.MethodPost)
	handleRefRoute(m, routeParams, s.Middleware(remClientH.DatasetPreviewHandler))

	routeParams = newrefRouteParams(lib.AEStatus, false, false, http.MethodGet, http.MethodPost)
	handleRefRoute(m, routeParams, s.Middleware(lib.NewHTTPRequestHandler(s.Instance, "fsi.status")))
	routeParams = newrefRouteParams(lib.AEWhatChanged, false, false, http.MethodGet, http.MethodPost)
	handleRefRoute(m, routeParams, s.Middleware(lib.NewHTTPRequestHandler(s.Instance, "fsi.whatchanged")))
	routeParams = newrefRouteParams(lib.AEInit, true, false, http.MethodPost)
	handleRefRoute(m, routeParams, s.Middleware(lib.NewHTTPRequestHandler(s.Instance, "fsi.init")))
	m.Handle(lib.AECanInitDatasetWorkDir.String(), s.Middleware(lib.NewHTTPRequestHandler(s.Instance, "fsi.caninitdatasetworkdir")))
	m.Handle(lib.AECheckout.String(), s.Middleware(lib.NewHTTPRequestHandler(s.Instance, "fsi.checkout"))).Methods(http.MethodPost)
	m.Handle(lib.AERestore.String(), s.Middleware(lib.NewHTTPRequestHandler(s.Instance, "fsi.restore"))).Methods(http.MethodPost)
	m.Handle(lib.AEFSIWrite.String(), s.Middleware(lib.NewHTTPRequestHandler(s.Instance, "fsi.write"))).Methods(http.MethodPost)
	m.Handle(lib.AEFSICreateLink.String(), s.Middleware(lib.NewHTTPRequestHandler(s.Instance, "fsi.createlink"))).Methods(http.MethodPost)
	m.Handle(lib.AEFSIUnlink.String(), s.Middleware(lib.NewHTTPRequestHandler(s.Instance, "fsi.unlink"))).Methods(http.MethodPost)

	renderh := NewRenderHandlers(s.Instance)
	routeParams = newrefRouteParams(lib.AERender, false, false, http.MethodGet, http.MethodPost)
	handleRefRoute(m, routeParams, s.Middleware(renderh.RenderHandler))

	m.Handle(lib.AEHistory.String(), s.Middleware(lib.NewHTTPRequestHandler(s.Instance, "log.history"))).Methods(http.MethodPost)
	m.Handle(lib.AEEntries.String(), s.Middleware(lib.NewHTTPRequestHandler(s.Instance, "log.entries"))).Methods(http.MethodPost)
	m.Handle(lib.AERawLogbook.String(), s.Middleware(lib.NewHTTPRequestHandler(s.Instance, "log.rawlogbook"))).Methods(http.MethodPost)
	m.Handle(lib.AELogbookSummary.String(), s.Middleware(lib.NewHTTPRequestHandler(s.Instance, "log.logbooksummary"))).Methods(http.MethodPost)

	rch := NewRegistryClientHandlers(s.Instance, cfg.API.ReadOnly)
	m.Handle(lib.AERegistryNew.String(), s.Middleware(rch.CreateProfileHandler))
	m.Handle(lib.AERegistryProve.String(), s.Middleware(rch.ProveProfileKeyHandler))

	m.Handle(lib.AESearch.String(), s.Middleware(lib.NewHTTPRequestHandler(s.Instance, "search.search"))).Methods(http.MethodPost)

	sqlh := NewSQLHandlers(s.Instance, cfg.API.ReadOnly)
	m.Handle(lib.AESQL.String(), s.Middleware(sqlh.QueryHandler))

	tfh := NewTransformHandlers(s.Instance)
	m.Handle(lib.AEApply.String(), s.Middleware(tfh.ApplyHandler(lib.AEApply.NoTrailingSlash())))

	if !cfg.API.DisableWebui {
		m.Handle(lib.AEWebUI.String(), s.Middleware(WebuiHandler))
	}

	return m
}
