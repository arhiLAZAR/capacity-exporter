package main

import (
	"fmt"
	"net/http"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

func serveExporter(config *configType) {
	var host, address, endpoint string
	var port int64

	host = config.Exporter.Host

	if config.Exporter.Port == 0 {
		port = exporterDefaultPort
	} else {
		port = config.Exporter.Port
	}

	if config.Exporter.MetricsEndpoint == "" {
		endpoint = "/metrics"
	} else {
		endpoint = config.Exporter.MetricsEndpoint
	}

	address = fmt.Sprintf("%s:%d", host, port)

	http.Handle(endpoint, promhttp.Handler())
	err := http.ListenAndServe(address, nil)
	checkErr(err)
}

func createGauge(name, help string, labels map[string]string) prometheus.Gauge {
	gauge := prometheus.NewGauge(
		prometheus.GaugeOpts{
			Name:        name,
			Namespace:   exporterNamespace,
			Help:        help,
			ConstLabels: labels,
		})
	prometheus.MustRegister(gauge)

	return gauge
}
