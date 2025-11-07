package main

import (
	"context"
	"fmt"
	"io"
	"log"
	"net"

	"github.com/ismail/weatherapp/analytics/weather"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type server struct {
	weather.UnimplementedAnalyticsServiceServer
	data []*weather.WeatherData
}

func (s *server) CollectWeatherData(stream grpc.ClientStreamingServer[weather.WeatherData, weather.WeatherStats]) error {
	var sumTemp, sumHum, sumPres float32
	count := 0

	for {
		wd, err := stream.Recv()
		if err == io.EOF {
			break
		}
		if err != nil {
			return status.Errorf(codes.Internal, "failed to receive data: %v", err)
		}

		if wd.Temperature > 100 {
			return status.Errorf(codes.InvalidArgument, "Temperature too high")
		}

		sumTemp += wd.Temperature
		sumHum += wd.Humidity
		sumPres += wd.Pressure
		count++
		s.data = append(s.data, wd)
	}

	stats := &weather.WeatherStats{
		AvgTemperature: sumTemp / float32(count),
		AvgHumidity:    sumHum / float32(count),
		AvgPressure:    sumPres / float32(count),
		SamplesCount:   int64(count),
	}

	return stream.SendAndClose(stats)
}

func (s *server) StreamAnalytics(_ *weather.Empty, stream weather.AnalyticsService_StreamAnalyticsServer) error {
	for _, wd := range s.data {
		stats := &weather.WeatherStats{
			AvgTemperature: wd.Temperature,
			AvgHumidity:    wd.Humidity,
			AvgPressure:    wd.Pressure,
			SamplesCount:   1,
		}
		if err := stream.Send(stats); err != nil {
			return err
		}
	}
	return nil
}

func (s *server) GetLastReport(ctx context.Context, _ *weather.Empty) (*weather.WeatherStats, error) {
	if len(s.data) == 0 {
		return nil, status.Error(codes.NotFound, "No data yet")
	}
	last := s.data[len(s.data)-1]
	return &weather.WeatherStats{
		AvgTemperature: last.Temperature,
		AvgHumidity:    last.Humidity,
		AvgPressure:    last.Pressure,
		SamplesCount:   1,
	}, nil
}

func main() {
	lis, err := net.Listen("tcp", ":50051")
	if err != nil {
		log.Fatalf("failed to listen: %v", err)
	}

	s := grpc.NewServer()
	weather.RegisterAnalyticsServiceServer(s, &server{})
	fmt.Println("Analytics Service running on port 50051...")

	if err := s.Serve(lis); err != nil {
		log.Fatalf("failed to serve: %v", err)
	}
}
