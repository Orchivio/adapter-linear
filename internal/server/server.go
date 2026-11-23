// Package server implements pluginv1.TrackerAdapterServer on top of Linear.
package server

import (
	"context"
	"time"

	pluginv1 "github.com/orchivio/orchestrator/contract/gen/orchivio/plugin/v1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/orchivio/adapter-linear/internal/linear"
	"github.com/orchivio/adapter-linear/internal/poller"
)

// Server is the TrackerAdapter gRPC service.
type Server struct {
	pluginv1.UnimplementedTrackerAdapterServer
	client   *linear.Client
	poller   *poller.Poller // process-level cursors per team key
	ctx      context.Context
	cancel   context.CancelFunc
	hub      *Hub
	webhooks bool
}

func New(client *linear.Client, hub *Hub, interval time.Duration, webhooks bool, now func() time.Time) *Server {
	ctx, cancel := context.WithCancel(context.Background())
	return &Server{client: client, poller: poller.New(client, now, interval), ctx: ctx, cancel: cancel, hub: hub, webhooks: webhooks}
}

func (s *Server) GetCapabilities(context.Context, *pluginv1.GetCapabilitiesRequest) (*pluginv1.Capabilities, error) {
	return &pluginv1.Capabilities{ContractVersion: "plugin.v1", AdapterName: "linear", SupportsWebhooks: s.webhooks}, nil
}

// StreamEvents polls the requested team keys until the client goes away.
func (s *Server) StreamEvents(req *pluginv1.StreamEventsRequest, stream pluginv1.TrackerAdapter_StreamEventsServer) error {
	teams := req.GetProjectKeys()
	if len(teams) == 0 {
		return status.Error(codes.InvalidArgument, "project_keys must not be empty")
	}
	nudge, unsubscribe := s.hub.Subscribe()
	defer unsubscribe()
	ctx, cancel := context.WithCancel(stream.Context())
	defer cancel()
	stop := context.AfterFunc(s.ctx, cancel) // server shutdown ends open streams
	defer stop()
	err := s.poller.Run(ctx, teams, nudge, stream.Send)
	if ctx.Err() != nil {
		return nil
	}
	return err
}

func (s *Server) GetTicket(ctx context.Context, req *pluginv1.GetTicketRequest) (*pluginv1.Ticket, error) {
	is, err := s.client.Issue(ctx, req.GetTicketRef())
	if err != nil {
		return nil, err
	}
	return &pluginv1.Ticket{TicketRef: is.Identifier, Title: is.Title, Description: is.Description, Labels: is.Labels, Url: is.URL}, nil
}

func (s *Server) PostComment(ctx context.Context, req *pluginv1.PostCommentRequest) (*pluginv1.PostCommentResponse, error) {
	id, err := s.client.CreateComment(ctx, req.GetTicketRef(), req.GetBody())
	if err != nil {
		return nil, err
	}
	return &pluginv1.PostCommentResponse{CommentId: id}, nil
}

// Close ends all open streams so GracefulStop does not wait for them.
func (s *Server) Close() { s.cancel() }
