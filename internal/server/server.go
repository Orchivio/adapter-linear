// Package server implements pluginv1.TrackerAdapterServer on top of Linear.
package server

import (
	"context"
	"time"

	pluginv1 "github.com/soemii/orchivio-server/contract/gen/orchivio/plugin/v1"

	"github.com/soemii/orchivio-adapter-linear/internal/linear"
	"github.com/soemii/orchivio-adapter-linear/internal/poller"
)

// Server is the TrackerAdapter gRPC service.
type Server struct {
	pluginv1.UnimplementedTrackerAdapterServer
	client   *linear.Client
	hub      *Hub
	teams    []string
	interval time.Duration
	webhooks bool
	now      func() time.Time
}

func New(client *linear.Client, hub *Hub, teams []string, interval time.Duration, webhooks bool, now func() time.Time) *Server {
	return &Server{client: client, hub: hub, teams: teams, interval: interval, webhooks: webhooks, now: now}
}

func (s *Server) GetCapabilities(context.Context, *pluginv1.GetCapabilitiesRequest) (*pluginv1.Capabilities, error) {
	return &pluginv1.Capabilities{ContractVersion: "plugin.v1", AdapterName: "linear", SupportsWebhooks: s.webhooks}, nil
}

// StreamEvents runs a dedicated poller per stream until the client goes away.
func (s *Server) StreamEvents(_ *pluginv1.StreamEventsRequest, stream pluginv1.TrackerAdapter_StreamEventsServer) error {
	nudge, unsubscribe := s.hub.Subscribe()
	defer unsubscribe()
	err := poller.New(s.client, s.teams, s.now(), s.interval).Run(stream.Context(), nudge, stream.Send)
	if stream.Context().Err() != nil {
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
