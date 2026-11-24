// Package server implements pluginv1.TrackerAdapterServer on top of Linear.
package server

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"slices"
	"time"

	pluginv1 "github.com/orchivio/orchestrator/contract/gen/orchivio/plugin/v1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/orchivio/adapter-linear/internal/linear"
	"github.com/orchivio/adapter-linear/internal/poller"
)

// refreshAfter is when the orchestrator renews a credential; Linear access tokens live 24h.
const refreshAfter = 12 * time.Hour

// Server is the TrackerAdapter gRPC service. It keeps no workspace access:
// every call carries the tenant's credential.
type Server struct {
	pluginv1.UnimplementedTrackerAdapterServer
	oauth    *linear.OAuth
	poller   *poller.Poller // process-level cursors per (workspace, team key)
	ctx      context.Context
	cancel   context.CancelFunc
	hub      *Hub
	webhooks bool
}

func New(oauth *linear.OAuth, hub *Hub, interval time.Duration, webhooks bool) *Server {
	ctx, cancel := context.WithCancel(context.Background())
	return &Server{oauth: oauth, poller: poller.New(oauth.Now, interval), ctx: ctx, cancel: cancel, hub: hub, webhooks: webhooks}
}

func (s *Server) GetCapabilities(context.Context, *pluginv1.GetCapabilitiesRequest) (*pluginv1.Capabilities, error) {
	return &pluginv1.Capabilities{ContractVersion: "plugin.v1", AdapterName: "linear", SupportsWebhooks: s.webhooks}, nil
}

func (s *Server) BeginConnect(_ context.Context, req *pluginv1.BeginConnectRequest) (*pluginv1.BeginConnectResponse, error) {
	if req.GetRedirectUri() == "" || req.GetState() == "" {
		return nil, status.Error(codes.InvalidArgument, "redirect_uri and state are required")
	}
	return &pluginv1.BeginConnectResponse{Url: s.oauth.AuthorizeURL(req.GetRedirectUri(), req.GetState())}, nil
}

// CompleteConnect handles the consent callback; a denied consent arrives without code.
func (s *Server) CompleteConnect(ctx context.Context, req *pluginv1.CompleteConnectRequest) (*pluginv1.CompleteConnectResponse, error) {
	code := req.GetQuery()["code"]
	if code == "" {
		return nil, status.Error(codes.InvalidArgument, "authorization code missing (consent denied?)")
	}
	cred, err := s.oauth.Exchange(ctx, req.GetRedirectUri(), code)
	if err != nil {
		return nil, grpcError(err)
	}
	return &pluginv1.CompleteConnectResponse{Credential: s.encode(cred), Label: cred.OrganizationName, ExternalId: cred.OrganizationID}, nil
}

func (s *Server) RefreshCredential(ctx context.Context, req *pluginv1.RefreshCredentialRequest) (*pluginv1.Credential, error) {
	cred, err := decode(req.GetCredential())
	if err != nil {
		return nil, err
	}
	if cred.RefreshToken == "" {
		return nil, status.Error(codes.Unauthenticated, "credential has no refresh token")
	}
	next, err := s.oauth.Refresh(ctx, cred)
	if err != nil {
		return nil, grpcError(err)
	}
	return s.encode(next), nil
}

// Disconnect revokes the grant at Linear; a failure is logged only, the
// orchestrator drops the credential either way.
func (s *Server) Disconnect(ctx context.Context, req *pluginv1.DisconnectRequest) (*pluginv1.DisconnectResponse, error) {
	cred, err := decode(req.GetCredential())
	if err != nil {
		return nil, err
	}
	if err := s.oauth.Revoke(ctx, cred.AccessToken); err != nil {
		slog.Warn("linear revoke failed", "organization", cred.OrganizationID, "err", err)
	}
	return &pluginv1.DisconnectResponse{}, nil
}

// CheckAccess denies team keys the workspace's app user cannot see.
func (s *Server) CheckAccess(ctx context.Context, req *pluginv1.CheckAccessRequest) (*pluginv1.CheckAccessResponse, error) {
	if len(req.GetCredentials()) == 0 {
		return nil, status.Error(codes.InvalidArgument, "credentials required")
	}
	cred, err := decode(req.GetCredentials()[0]) // one Linear workspace per tenant
	if err != nil {
		return nil, err
	}
	keys, err := s.oauth.Client(cred.AccessToken).TeamKeys(ctx)
	if err != nil {
		return nil, grpcError(err)
	}
	var denied []string
	for _, item := range req.GetItems() {
		if !slices.Contains(keys, item) {
			denied = append(denied, item)
		}
	}
	return &pluginv1.CheckAccessResponse{Denied: denied}, nil
}

// StreamEvents polls the requested team keys until the client goes away.
func (s *Server) StreamEvents(req *pluginv1.StreamEventsRequest, stream pluginv1.TrackerAdapter_StreamEventsServer) error {
	teams := req.GetProjectKeys()
	if len(teams) == 0 {
		return status.Error(codes.InvalidArgument, "project_keys must not be empty")
	}
	cred, err := decode(req.GetCredential())
	if err != nil {
		return err
	}
	nudge, unsubscribe := s.hub.Subscribe()
	defer unsubscribe()
	ctx, cancel := context.WithCancel(stream.Context())
	defer cancel()
	stop := context.AfterFunc(s.ctx, cancel) // server shutdown ends open streams
	defer stop()
	// ponytail: an expired token keeps failing (logged) until the orchestrator reopens the stream with a refreshed credential.
	err = s.poller.Run(ctx, s.oauth.Client(cred.AccessToken), cred.OrganizationID, teams, nudge, stream.Send)
	if ctx.Err() != nil {
		return nil
	}
	return err
}

func (s *Server) GetTicket(ctx context.Context, req *pluginv1.GetTicketRequest) (*pluginv1.Ticket, error) {
	cred, err := decode(req.GetCredential())
	if err != nil {
		return nil, err
	}
	is, err := s.oauth.Client(cred.AccessToken).Issue(ctx, req.GetTicketRef())
	if err != nil {
		return nil, grpcError(err)
	}
	return &pluginv1.Ticket{TicketRef: is.Identifier, Title: is.Title, Description: is.Description, Labels: is.Labels, Url: is.URL}, nil
}

func (s *Server) PostComment(ctx context.Context, req *pluginv1.PostCommentRequest) (*pluginv1.PostCommentResponse, error) {
	cred, err := decode(req.GetCredential())
	if err != nil {
		return nil, err
	}
	id, err := s.oauth.Client(cred.AccessToken).CreateComment(ctx, req.GetTicketRef(), req.GetBody())
	if err != nil {
		return nil, grpcError(err)
	}
	return &pluginv1.PostCommentResponse{CommentId: id}, nil
}

// Close ends all open streams so GracefulStop does not wait for them.
func (s *Server) Close() { s.cancel() }

func (s *Server) encode(cred linear.Credential) *pluginv1.Credential {
	data, _ := json.Marshal(cred) // strings and a time: cannot fail
	return &pluginv1.Credential{Data: data, RefreshAfter: timestamppb.New(s.oauth.Now().Add(refreshAfter))}
}

// decode never echoes the data: it holds tokens.
func decode(c *pluginv1.Credential) (linear.Credential, error) {
	var cred linear.Credential
	if len(c.GetData()) == 0 {
		return cred, status.Error(codes.InvalidArgument, "credential required")
	}
	if json.Unmarshal(c.GetData(), &cred) != nil || cred.AccessToken == "" {
		return cred, status.Error(codes.InvalidArgument, "credential malformed")
	}
	return cred, nil
}

func grpcError(err error) error {
	switch {
	case errors.Is(err, linear.ErrUnauthenticated):
		return status.Error(codes.Unauthenticated, err.Error())
	case errors.Is(err, linear.ErrNotFound):
		return status.Error(codes.NotFound, err.Error())
	default:
		return status.Error(codes.Unavailable, err.Error())
	}
}
