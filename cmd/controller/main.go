package main

import (
	"context"
	"errors"
	"os"

	rolloutsv1alpha1 "github.com/argoproj/argo-rollouts/pkg/apis/rollouts/v1alpha1"
	apimeta "k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/runtime"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/healthz"
	"sigs.k8s.io/controller-runtime/pkg/log/zap"
	metricsserver "sigs.k8s.io/controller-runtime/pkg/metrics/server"

	"github.com/aeyrtonvs/k8s-drain-surge/internal/config"
	"github.com/aeyrtonvs/k8s-drain-surge/internal/controller"
)

// rolloutsCRDPresent probes the RESTMapper for the Argo Rollouts CRD. The
// manager registers the rollouts scheme unconditionally, but if the CRD is
// not installed in the cluster the informer fails to start. Karpenter-surge
// is supposed to work on Deployment-only clusters, so we detect the absence
// here and skip Rollout-side watches/lists in the affected reconciler.
func rolloutsCRDPresent(mgr ctrl.Manager) bool {
	_, err := mgr.GetRESTMapper().RESTMapping(
		rolloutsv1alpha1.SchemeGroupVersion.WithKind("Rollout").GroupKind(),
		rolloutsv1alpha1.SchemeGroupVersion.Version,
	)
	if err == nil {
		return true
	}
	var noMatch *apimeta.NoKindMatchError
	return !errors.As(err, &noMatch)
}

var scheme = runtime.NewScheme()

func init() {
	utilruntime.Must(clientgoscheme.AddToScheme(scheme))
	utilruntime.Must(rolloutsv1alpha1.AddToScheme(scheme))
}

func main() {
	opts := zap.Options{Development: false}
	ctrl.SetLogger(zap.New(zap.UseFlagOptions(&opts)))
	log := ctrl.Log.WithName("setup")

	cfg, err := config.Parse()
	if err != nil {
		log.Error(err, "invalid configuration")
		os.Exit(1)
	}

	mgr, err := ctrl.NewManager(ctrl.GetConfigOrDie(), ctrl.Options{
		Scheme:                 scheme,
		LeaderElection:         cfg.LeaderElect,
		LeaderElectionID:       controller.ControllerName + "-leader",
		HealthProbeBindAddress: cfg.HealthAddr,
		Metrics: metricsserver.Options{
			BindAddress: cfg.MetricsAddr,
		},
	})
	if err != nil {
		log.Error(err, "unable to create manager")
		os.Exit(1)
	}

	if err := mgr.AddHealthzCheck("healthz", healthz.Ping); err != nil {
		log.Error(err, "unable to set up health check")
		os.Exit(1)
	}
	if err := mgr.AddReadyzCheck("readyz", healthz.Ping); err != nil {
		log.Error(err, "unable to set up ready check")
		os.Exit(1)
	}

	reconciler := &controller.NodeReconciler{
		Client:   mgr.GetClient(),
		Scheme:   mgr.GetScheme(),
		Recorder: mgr.GetEventRecorderFor(controller.ControllerName),
		Config:   cfg,
	}

	if err := reconciler.SetupWithManager(mgr); err != nil {
		log.Error(err, "unable to create controller")
		os.Exit(1)
	}

	var restartReconciler *controller.RolloutReconciler
	if cfg.RestartSurgeEnabled {
		restartReconciler = &controller.RolloutReconciler{
			Client:   mgr.GetClient(),
			Scheme:   mgr.GetScheme(),
			Recorder: mgr.GetEventRecorderFor(controller.ControllerName),
			Config:   cfg,
		}
		if err := restartReconciler.SetupWithManager(mgr); err != nil {
			log.Error(err, "unable to create rollout restart-surge controller")
			os.Exit(1)
		}
		log.Info("restart-surge protection enabled")
	}

	var karpenterReconciler *controller.KarpenterSurgeReconciler
	if cfg.KarpenterSurgeEnabled {
		rolloutsPresent := rolloutsCRDPresent(mgr)
		if !rolloutsPresent {
			log.Info("karpenter-surge: Argo Rollouts CRD not installed, restricting to Deployments")
		}
		karpenterReconciler = &controller.KarpenterSurgeReconciler{
			Client:           mgr.GetClient(),
			Scheme:           mgr.GetScheme(),
			Recorder:         mgr.GetEventRecorderFor(controller.ControllerName),
			Config:           cfg,
			RolloutsAvailable: rolloutsPresent,
		}
		if err := karpenterReconciler.SetupWithManager(mgr); err != nil {
			log.Error(err, "unable to create karpenter-surge controller")
			os.Exit(1)
		}
		log.Info("karpenter-surge protection enabled")
	}

	go func() {
		<-mgr.Elected()
		ctx := context.Background()
		if err := reconciler.RecoverOrphans(ctx); err != nil {
			log.Error(err, "orphan recovery failed")
		}
		if restartReconciler != nil {
			if err := restartReconciler.RecoverOrphans(ctx); err != nil {
				log.Error(err, "restart-surge orphan recovery failed")
			}
		}
		if karpenterReconciler != nil {
			if err := karpenterReconciler.RecoverOrphans(ctx); err != nil {
				log.Error(err, "karpenter-surge orphan recovery failed")
			}
		}
	}()

	log.Info("starting manager")
	if err := mgr.Start(ctrl.SetupSignalHandler()); err != nil {
		log.Error(err, "manager exited with error")
		os.Exit(1)
	}
}
