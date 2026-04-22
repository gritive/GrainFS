// Package otel provides OTel trace provider initialization for GrainFS.
// Head-based 1% sampling is the default; operators override via --otel-sample-rate.
package otel

import (
	"context"
	"fmt"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracehttp"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.26.0"
	"go.opentelemetry.io/otel/trace"
)

// Tracer is the package-level tracer name used by the scrubber.
const Tracer = "grainfs/scrubber"

// nopShutdown is returned when OTel is disabled.
var nopShutdown = func(context.Context) error { return nil }

// Init initialises the global OTel trace provider and returns a shutdown
// function. When endpoint is empty the global provider is left as the SDK
// nop provider (no spans exported). sampleRate is in the range [0.0, 1.0].
func Init(ctx context.Context, endpoint string, sampleRate float64) (shutdown func(context.Context) error, err error) {
	if endpoint == "" {
		return nopShutdown, nil
	}

	exp, err := otlptracehttp.New(ctx,
		otlptracehttp.WithEndpoint(endpoint),
		otlptracehttp.WithInsecure(),
	)
	if err != nil {
		return nil, fmt.Errorf("otel: create exporter: %w", err)
	}

	res, err := resource.New(ctx,
		resource.WithAttributes(semconv.ServiceName("grainfs")),
		resource.WithFromEnv(),
	)
	if err != nil {
		res = resource.Default()
	}

	if sampleRate < 0 {
		sampleRate = 0
	}
	if sampleRate > 1 {
		sampleRate = 1
	}

	tp := sdktrace.NewTracerProvider(
		sdktrace.WithBatcher(exp),
		sdktrace.WithResource(res),
		sdktrace.WithSampler(sdktrace.ParentBased(sdktrace.TraceIDRatioBased(sampleRate))),
	)
	otel.SetTracerProvider(tp)

	return tp.Shutdown, nil
}

// Get returns the package-level tracer, using the global provider. If Init
// was not called (or endpoint was empty) this returns the nop tracer.
func Get() trace.Tracer {
	return otel.GetTracerProvider().Tracer(Tracer)
}
