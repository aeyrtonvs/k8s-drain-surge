package controller

import (
	"testing"
	"time"

	rolloutsv1alpha1 "github.com/argoproj/argo-rollouts/pkg/apis/rollouts/v1alpha1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func ts(t time.Time) *metav1.Time {
	m := metav1.NewTime(t)
	return &m
}

func mkRollout(restartAt, restartedAt *metav1.Time) *rolloutsv1alpha1.Rollout {
	ro := &rolloutsv1alpha1.Rollout{}
	if restartAt != nil {
		ro.Spec.RestartAt = restartAt
	}
	if restartedAt != nil {
		ro.Status.RestartedAt = restartedAt
	}
	return ro
}

func mkPod(name string, created time.Time, terminating bool) corev1.Pod {
	p := corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:              name,
			CreationTimestamp: metav1.NewTime(created),
		},
	}
	if terminating {
		now := metav1.NewTime(time.Now())
		p.ObjectMeta.DeletionTimestamp = &now
	}
	return p
}

func TestIsRestartStuck(t *testing.T) {
	now := time.Date(2026, 5, 16, 12, 0, 0, 0, time.UTC)
	grace := 60 * time.Second

	restartAt := now.Add(-5 * time.Minute) // 5 min ago, past grace
	beforeRestart := restartAt.Add(-1 * time.Hour)
	afterRestart := restartAt.Add(1 * time.Second)

	tests := []struct {
		name   string
		ro     *rolloutsv1alpha1.Rollout
		pods   []corev1.Pod
		now    time.Time
		expect bool
	}{
		{
			name:   "no restartAt set",
			ro:     mkRollout(nil, nil),
			pods:   []corev1.Pod{mkPod("p1", beforeRestart, false)},
			now:    now,
			expect: false,
		},
		{
			name:   "Argo already completed (status matches spec)",
			ro:     mkRollout(ts(restartAt), ts(restartAt)),
			pods:   []corev1.Pod{mkPod("p1", beforeRestart, false)},
			now:    now,
			expect: false,
		},
		{
			name:   "restartAt in the future",
			ro:     mkRollout(ts(now.Add(1*time.Minute)), nil),
			pods:   []corev1.Pod{mkPod("p1", beforeRestart, false)},
			now:    now,
			expect: false,
		},
		{
			name:   "within grace period",
			ro:     mkRollout(ts(now.Add(-30*time.Second)), nil),
			pods:   []corev1.Pod{mkPod("p1", beforeRestart, false)},
			now:    now,
			expect: false,
		},
		{
			name:   "stuck: old pod past grace",
			ro:     mkRollout(ts(restartAt), nil),
			pods:   []corev1.Pod{mkPod("p1", beforeRestart, false)},
			now:    now,
			expect: true,
		},
		{
			name:   "stuck: status.restartedAt stale (not equal to spec)",
			ro:     mkRollout(ts(restartAt), ts(restartAt.Add(-1*time.Hour))),
			pods:   []corev1.Pod{mkPod("p1", beforeRestart, false)},
			now:    now,
			expect: true,
		},
		{
			name:   "not stuck: all pods are post-restart",
			ro:     mkRollout(ts(restartAt), nil),
			pods:   []corev1.Pod{mkPod("p1", afterRestart, false)},
			now:    now,
			expect: false,
		},
		{
			name:   "not stuck: old pod is terminating",
			ro:     mkRollout(ts(restartAt), nil),
			pods:   []corev1.Pod{mkPod("p1", beforeRestart, true)},
			now:    now,
			expect: false,
		},
		{
			name: "stuck: mix of post-restart and terminating old plus one stale",
			ro:   mkRollout(ts(restartAt), nil),
			pods: []corev1.Pod{
				mkPod("p-new", afterRestart, false),
				mkPod("p-term", beforeRestart, true),
				mkPod("p-stale", beforeRestart, false),
			},
			now:    now,
			expect: true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := isRestartStuck(tc.ro, tc.pods, grace, tc.now)
			if got != tc.expect {
				t.Fatalf("expected %v, got %v", tc.expect, got)
			}
		})
	}
}

func TestRestartCompletedByArgo(t *testing.T) {
	now := time.Now()
	restartAt := now.Add(-5 * time.Minute)

	tests := []struct {
		name   string
		ro     *rolloutsv1alpha1.Rollout
		pods   []corev1.Pod
		expect bool
	}{
		{
			name:   "no restartAt → trivially complete",
			ro:     mkRollout(nil, nil),
			pods:   nil,
			expect: true,
		},
		{
			name:   "status matches spec",
			ro:     mkRollout(ts(restartAt), ts(restartAt)),
			pods:   []corev1.Pod{mkPod("p1", restartAt.Add(-time.Hour), false)},
			expect: true,
		},
		{
			name:   "still has old non-terminating pod",
			ro:     mkRollout(ts(restartAt), nil),
			pods:   []corev1.Pod{mkPod("p1", restartAt.Add(-time.Hour), false)},
			expect: false,
		},
		{
			name: "only post-restart pods remain",
			ro:   mkRollout(ts(restartAt), nil),
			pods: []corev1.Pod{
				mkPod("p1", restartAt.Add(1*time.Second), false),
				mkPod("p2", restartAt.Add(2*time.Second), false),
			},
			expect: true,
		},
		{
			name: "old pod is terminating → counts as complete enough",
			ro:   mkRollout(ts(restartAt), nil),
			pods: []corev1.Pod{
				mkPod("p1", restartAt.Add(-time.Hour), true),
				mkPod("p2", restartAt.Add(1*time.Second), false),
			},
			expect: true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := restartCompletedByArgo(tc.ro, tc.pods)
			if got != tc.expect {
				t.Fatalf("expected %v, got %v", tc.expect, got)
			}
		})
	}
}
