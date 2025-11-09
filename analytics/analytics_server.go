package main

import (
	"context"
	"fmt"
	"io"
	"log"
	"net"
	"sync"
	"time"

	"github.com/ismail/weatherapp/weather"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

const (
	port          = ":5051"
	expectedToken = "MY_SECRET_TOKEN"
)

type server struct {
	weather.UnimplementedAnalyticsServiceServer
	mu        sync.RWMutex
	data      []*weather.WeatherData
	lastStats *weather.WeatherStats
}

func newServer() *server {
	return &server{data: []*weather.WeatherData{}}
}

func computeStats(samples []*weather.WeatherData) *weather.WeatherStats {
	var sumT, sumH, sumP float32
	var count int64
	for _, s := range samples {
		sumT += s.Temperature
		sumH += s.Humidity
		sumP += s.Pressure
		count++
	}
	if count == 0 {
		return &weather.WeatherStats{}
	}
	return &weather.WeatherStats{
		AvgTemperature: sumT / float32(count),
		AvgHumidity:    sumH / float32(count),
		AvgPressure:    sumP / float32(count),
		SamplesCount:   count,
	}
}

func (s *server) CollectWeatherData(stream weather.AnalyticsService_CollectWeatherDataServer) error {
	md, ok := metadata.FromIncomingContext(stream.Context())
	if !ok {
		return status.Error(codes.Unauthenticated, "missing metadata")
	}
	tokens := md.Get("token")
	if len(tokens) == 0 || tokens[0] != expectedToken {
		return status.Error(codes.Unauthenticated, "invalid token")
	}

	log.Println(" Client connected: token OK")

	for {
		wd, err := stream.Recv()
		if err == io.EOF {
			s.mu.RLock()
			stats := s.lastStats
			s.mu.RUnlock()
			return stream.SendAndClose(stats)
		}
		if err != nil {
			return err
		}

		if wd.Temperature > 45 {
			return status.Errorf(codes.OutOfRange, "Temperature too high: %.2f°C", wd.Temperature)
		}
		if wd.Humidity < 10 {
			return status.Errorf(codes.OutOfRange, "Humidity too low: %.2f%%", wd.Humidity)
		}
		if wd.Pressure < 950 || wd.Pressure > 1050 {
			return status.Errorf(codes.OutOfRange, "Pressure out of range: %.2f hPa", wd.Pressure)
		}

		s.mu.Lock()
		s.data = append(s.data, wd)
		s.lastStats = computeStats(s.data)
		s.mu.Unlock()

		log.Println("Received data:", wd)
	}
}

func (s *server) StreamAnalytics(_ *weather.Empty, stream weather.AnalyticsService_StreamAnalyticsServer) error {
	ticker := time.NewTicker(3 * time.Second)
	defer ticker.Stop()
	log.Println("Dashboard connected")

	for {
		select {
		case <-stream.Context().Done():
			log.Println("Dashboard disconnected")
			return nil
		case <-ticker.C:
			s.mu.RLock()
			stats := s.lastStats
			s.mu.RUnlock()
			if stats != nil {
				if err := stream.Send(stats); err != nil {
					return err
				}
			}
		}
	}
}

func (s *server) GetLastReport(ctx context.Context, _ *weather.Empty) (*weather.WeatherStats, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if s.lastStats == nil {
		return nil, status.Error(codes.NotFound, "no stats yet")
	}
	return s.lastStats, nil
}

func main() {
	lis, err := net.Listen("tcp", port)
	if err != nil {
		log.Fatalf("failed to listen: %v", err)
	}

	grpcServer := grpc.NewServer()
	srv := newServer()
	weather.RegisterAnalyticsServiceServer(grpcServer, srv)

	fmt.Println(" Analytics Service running on 10.84.77.209:5051")
	if err := grpcServer.Serve(lis); err != nil {
		log.Fatalf("failed to serve: %v", err)
	}
}
