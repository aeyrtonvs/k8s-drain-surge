package config

import (
	"flag"
	"fmt"
	"time"
)

type DrainTaint struct {
	Key    string
	Effect string
}

type Config struct {
	DrainTaints             []DrainTaint
	EnabledAnnotation       string
	RequeueInterval         time.Duration
	ReadinessTimeout        time.Duration
	LeaderElect             bool
	MetricsAddr             string
	HealthAddr              string
	RestartSurgeEnabled     bool
	RestartSurgeGracePeriod time.Duration
	RestartSurgeTimeout     time.Duration

	KarpenterSurgeEnabled     bool
	KarpenterSurgeGracePeriod time.Duration
	KarpenterSurgeTimeout     time.Duration
	KarpenterSurgeScanPeriod  time.Duration
}

func DefaultDrainTaints() []DrainTaint {
	return []DrainTaint{
		{Key: "karpenter.sh/disrupted", Effect: "NoSchedule"},
		{Key: "ToBeDeletedByClusterAutoscaler", Effect: "NoSchedule"},
		{Key: "node.kubernetes.io/unschedulable", Effect: "NoSchedule"},
	}
}

func Parse() (*Config, error) {
	cfg := &Config{
		DrainTaints: DefaultDrainTaints(),
	}

	flag.StringVar(&cfg.EnabledAnnotation, "enabled-annotation", "k8s-drain-surge.io/enabled", "Annotation opt-in key")
	flag.DurationVar(&cfg.RequeueInterval, "requeue-interval", 5*time.Second, "Reconciliation requeue interval")
	flag.DurationVar(&cfg.ReadinessTimeout, "readiness-timeout", 10*time.Minute, "Timeout waiting for new pod readiness")
	flag.BoolVar(&cfg.LeaderElect, "leader-elect", true, "Enable leader election for HA")
	flag.StringVar(&cfg.MetricsAddr, "metrics-addr", ":8080", "Prometheus metrics bind address")
	flag.StringVar(&cfg.HealthAddr, "health-addr", ":8081", "Health probe bind address")
	flag.BoolVar(&cfg.RestartSurgeEnabled, "restart-surge-enabled", false, "Enable restart-surge protection for Argo Rollouts blocked by PDB during restart")
	flag.DurationVar(&cfg.RestartSurgeGracePeriod, "restart-surge-grace-period", 60*time.Second, "Grace period after spec.restartAt before triggering restart-surge (lets Argo restart normally when PDB allows)")
	flag.DurationVar(&cfg.RestartSurgeTimeout, "restart-surge-timeout", 10*time.Minute, "Timeout for the full restart-surge operation")
	flag.BoolVar(&cfg.KarpenterSurgeEnabled, "karpenter-surge-enabled", true, "Enable karpenter-surge protection for single-replica workloads whose PDB blocks Karpenter consolidation pre-taint (default on — the PDB this controller requires is the very mechanism that triggers the Karpenter stuck case)")
	flag.DurationVar(&cfg.KarpenterSurgeGracePeriod, "karpenter-surge-grace-period", 60*time.Second, "Time PDB must stay at disruptionsAllowed=0 before triggering karpenter-surge (~6 Karpenter pollingPeriod cycles)")
	flag.DurationVar(&cfg.KarpenterSurgeTimeout, "karpenter-surge-timeout", 10*time.Minute, "Timeout for the full karpenter-surge operation")
	flag.DurationVar(&cfg.KarpenterSurgeScanPeriod, "karpenter-surge-scan-period", 60*time.Second, "Period of the absolute backup ticker that scans PDBs cluster-wide for stuck workloads (covers informer desync)")
	flag.Parse()

	if err := cfg.Validate(); err != nil {
		return nil, err
	}

	return cfg, nil
}

func (c *Config) Validate() error {
	if c.RequeueInterval <= 0 {
		return fmt.Errorf("--requeue-interval must be positive, got %s", c.RequeueInterval)
	}
	if c.ReadinessTimeout <= 0 {
		return fmt.Errorf("--readiness-timeout must be positive, got %s", c.ReadinessTimeout)
	}
	if c.ReadinessTimeout <= c.RequeueInterval {
		return fmt.Errorf("--readiness-timeout (%s) must be greater than --requeue-interval (%s)", c.ReadinessTimeout, c.RequeueInterval)
	}
	if len(c.DrainTaints) == 0 {
		return fmt.Errorf("at least one drain taint must be configured")
	}
	if c.EnabledAnnotation == "" {
		return fmt.Errorf("--enabled-annotation must not be empty")
	}
	if c.RestartSurgeEnabled {
		if c.RestartSurgeGracePeriod <= 0 {
			return fmt.Errorf("--restart-surge-grace-period must be positive, got %s", c.RestartSurgeGracePeriod)
		}
		if c.RestartSurgeTimeout <= c.RestartSurgeGracePeriod {
			return fmt.Errorf("--restart-surge-timeout (%s) must be greater than --restart-surge-grace-period (%s)", c.RestartSurgeTimeout, c.RestartSurgeGracePeriod)
		}
	}
	if c.KarpenterSurgeEnabled {
		if c.KarpenterSurgeGracePeriod <= 0 {
			return fmt.Errorf("--karpenter-surge-grace-period must be positive, got %s", c.KarpenterSurgeGracePeriod)
		}
		if c.KarpenterSurgeTimeout <= c.KarpenterSurgeGracePeriod {
			return fmt.Errorf("--karpenter-surge-timeout (%s) must be greater than --karpenter-surge-grace-period (%s)", c.KarpenterSurgeTimeout, c.KarpenterSurgeGracePeriod)
		}
		if c.KarpenterSurgeScanPeriod < c.RequeueInterval {
			return fmt.Errorf("--karpenter-surge-scan-period (%s) must be >= --requeue-interval (%s)", c.KarpenterSurgeScanPeriod, c.RequeueInterval)
		}
	}
	return nil
}
