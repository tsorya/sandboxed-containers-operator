package controllers

import (
	"context"
	"maps"
	"reflect"
	"strings"
	"testing"

	"github.com/go-logr/logr"
	mcfgv1 "github.com/openshift/api/machineconfiguration/v1"
	kataconfigurationv1 "github.com/openshift/sandboxed-containers-operator/api/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/util/workqueue"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/event"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
)

func externalMachineConfigPool(name string) *mcfgv1.MachineConfigPool {
	return &mcfgv1.MachineConfigPool{
		ObjectMeta: metav1.ObjectMeta{
			Name: name,
			Labels: map[string]string{
				"app.kubernetes.io/name":    "dpu-worker-config",
				"app.kubernetes.io/part-of": "dpf-hcp-provisioner-operator",
			},
		},
		Spec: mcfgv1.MachineConfigPoolSpec{
			MachineConfigSelector: &metav1.LabelSelector{
				MatchExpressions: []metav1.LabelSelectorRequirement{{
					Key:      "machineconfiguration.openshift.io/role",
					Operator: metav1.LabelSelectorOpIn,
					Values:   []string{"worker", name},
				}},
			},
			NodeSelector: &metav1.LabelSelector{
				MatchLabels: map[string]string{nodeRoleLabelPrefix + name: ""},
			},
			Paused: false,
		},
	}
}

func externalMachineConfigPoolReconciler(t *testing.T, objects ...runtime.Object) *KataConfigOpenShiftReconciler {
	t.Helper()

	scheme := runtime.NewScheme()
	if err := corev1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	if err := mcfgv1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	if err := kataconfigurationv1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}

	return &KataConfigOpenShiftReconciler{
		Client: fake.NewClientBuilder().WithScheme(scheme).
			WithStatusSubresource(&kataconfigurationv1.KataConfig{}).
			WithRuntimeObjects(objects...).Build(),
		Log:    logr.Discard(),
		Scheme: scheme,
		kataConfig: &kataconfigurationv1.KataConfig{
			ObjectMeta: metav1.ObjectMeta{Name: "cluster-kataconfig"},
			Spec: kataconfigurationv1.KataConfigSpec{
				TargetMachineConfigPool: "worker-dpu",
			},
		},
	}
}

func TestExternalMachineConfigPoolTargeting(t *testing.T) {
	t.Setenv("SANDBOXED_CONTAINERS_EXTENSION", "sandboxed-containers")

	r := externalMachineConfigPoolReconciler(t)

	machinePool, err := r.getMcpName()
	if err != nil {
		t.Fatalf("unexpected error resolving target MCP: %v", err)
	}
	if machinePool != "worker-dpu" {
		t.Fatalf("expected worker-dpu MCP, got %q", machinePool)
	}

	expectedNodeSelector := map[string]string{"node-role.kubernetes.io/worker-dpu": ""}
	if actual := r.getNodeSelectorAsMap(); !reflect.DeepEqual(actual, expectedNodeSelector) {
		t.Fatalf("unexpected node selector: %#v", actual)
	}

	mc, err := r.newMCForCR(machinePool, nil)
	if err != nil {
		t.Fatalf("unexpected error generating MachineConfig: %v", err)
	}
	if actual := mc.Labels["machineconfiguration.openshift.io/role"]; actual != machinePool {
		t.Fatalf("expected MachineConfig role %q, got %q", machinePool, actual)
	}
}

func TestEnsureExternalMachineConfigPool(t *testing.T) {
	t.Run("accepts DPF pool and adds only the selector label", func(t *testing.T) {
		mcp := externalMachineConfigPool("worker-dpu")
		expectedSpec := mcp.Spec.DeepCopy()
		expectedLabels := maps.Clone(mcp.Labels)
		r := externalMachineConfigPoolReconciler(t, mcp)

		if err := r.ensureExternalMachineConfigPool(); err != nil {
			t.Fatalf("unexpected error ensuring external MCP: %v", err)
		}

		actual := &mcfgv1.MachineConfigPool{}
		if err := r.Get(context.Background(), types.NamespacedName{Name: mcp.Name}, actual); err != nil {
			t.Fatal(err)
		}
		if !reflect.DeepEqual(&actual.Spec, expectedSpec) {
			t.Fatalf("external MCP spec was changed: %#v", actual.Spec)
		}
		for key, value := range expectedLabels {
			if actual.Labels[key] != value {
				t.Fatalf("external MCP label %q was changed", key)
			}
		}
		if value, ok := actual.Labels[externalMCPSelectorLabel]; !ok || value != r.kataConfig.Name {
			t.Fatalf("missing ContainerRuntimeConfig selector label: %#v", actual.Labels)
		}
		if err := r.removeExternalMachineConfigPoolSelectorLabel(); err != nil {
			t.Fatalf("unexpected error removing external MCP selector label: %v", err)
		}
		if err := r.Get(context.Background(), types.NamespacedName{Name: mcp.Name}, actual); err != nil {
			t.Fatal(err)
		}
		if !reflect.DeepEqual(actual.Labels, expectedLabels) {
			t.Fatalf("external MCP labels were not restored: %#v", actual.Labels)
		}
	})

	t.Run("does not overwrite a selector label owned by another KataConfig", func(t *testing.T) {
		mcp := externalMachineConfigPool("worker-dpu")
		mcp.Labels[externalMCPSelectorLabel] = "another-kataconfig"
		r := externalMachineConfigPoolReconciler(t, mcp)

		err := r.ensureExternalMachineConfigPool()
		if err == nil || !strings.Contains(err.Error(), "owned by") {
			t.Fatalf("expected selector label ownership error, got %v", err)
		}
	})

	t.Run("rejects a pool that does not select its role", func(t *testing.T) {
		mcp := externalMachineConfigPool("worker-dpu")
		mcp.Spec.MachineConfigSelector.MatchExpressions[0].Values = []string{"worker"}
		r := externalMachineConfigPoolReconciler(t, mcp)

		err := r.ensureExternalMachineConfigPool()
		if err == nil || !strings.Contains(err.Error(), "does not select MachineConfigs") {
			t.Fatalf("expected role selector error, got %v", err)
		}
	})

	t.Run("rejects a pool without its node role", func(t *testing.T) {
		mcp := externalMachineConfigPool("worker-dpu")
		mcp.Spec.NodeSelector.MatchLabels = map[string]string{"dpu-enabled": "true"}
		r := externalMachineConfigPoolReconciler(t, mcp)

		err := r.ensureExternalMachineConfigPool()
		if err == nil || !strings.Contains(err.Error(), "must select nodes with label") {
			t.Fatalf("expected node selector error, got %v", err)
		}
	})

	t.Run("rejects a paused pool", func(t *testing.T) {
		mcp := externalMachineConfigPool("worker-dpu")
		mcp.Spec.Paused = true
		r := externalMachineConfigPoolReconciler(t, mcp)

		err := r.ensureExternalMachineConfigPool()
		if err == nil || !strings.Contains(err.Error(), "is paused") {
			t.Fatalf("expected paused pool error, got %v", err)
		}
	})

	t.Run("rejects node eligibility filtering", func(t *testing.T) {
		mcp := externalMachineConfigPool("worker-dpu")
		r := externalMachineConfigPoolReconciler(t, mcp)
		r.kataConfig.Spec.CheckNodeEligibility = true

		err := r.ensureExternalMachineConfigPool()
		if err == nil || !strings.Contains(err.Error(), "checkNodeEligibility") {
			t.Fatalf("expected node eligibility error, got %v", err)
		}
	})

	t.Run("rejects a KataConfig node selector", func(t *testing.T) {
		mcp := externalMachineConfigPool("worker-dpu")
		r := externalMachineConfigPoolReconciler(t, mcp)
		r.kataConfig.Spec.KataConfigPoolSelector = &metav1.LabelSelector{
			MatchLabels: map[string]string{"dpu-enabled": "true"},
		}

		err := r.ensureExternalMachineConfigPool()
		if err == nil || !strings.Contains(err.Error(), "kataConfigPoolSelector") {
			t.Fatalf("expected KataConfig pool selector error, got %v", err)
		}
	})

	t.Run("returns an actionable error when the pool is missing", func(t *testing.T) {
		r := externalMachineConfigPoolReconciler(t)

		err := r.ensureExternalMachineConfigPool()
		if err == nil || !strings.Contains(err.Error(), `target MachineConfigPool "worker-dpu"`) {
			t.Fatalf("expected missing target MCP error, got %v", err)
		}
	})
}

func TestTargetMachineConfigPoolNotFoundCondition(t *testing.T) {
	t.Run("persists the condition when installation cannot find the pool", func(t *testing.T) {
		r := externalMachineConfigPoolReconciler(t)
		if err := r.Create(context.Background(), r.kataConfig); err != nil {
			t.Fatal(err)
		}

		if _, err := r.processKataConfigInstallRequest(); err == nil || !strings.Contains(err.Error(), "not found") {
			t.Fatalf("expected missing target MCP error, got %v", err)
		}
		if condition := r.findInProgressCondition(); condition == nil || condition.Reason != targetMcpNotFoundReason {
			t.Fatalf("expected in-memory missing target MCP condition, got %#v", condition)
		}

		stored := &kataconfigurationv1.KataConfig{}
		if err := r.Get(context.Background(), types.NamespacedName{Name: r.kataConfig.Name}, stored); err != nil {
			t.Fatal(err)
		}
		if len(stored.Status.Conditions) != 1 {
			t.Fatalf("expected one persisted condition, got %#v", stored.Status.Conditions)
		}
		condition := stored.Status.Conditions[0]
		if condition.Status != corev1.ConditionTrue || condition.Reason != targetMcpNotFoundReason {
			t.Fatalf("unexpected persisted condition: %#v", condition)
		}
	})

	t.Run("records the missing pool and recovers to installing", func(t *testing.T) {
		r := externalMachineConfigPoolReconciler(t)
		missingPoolErr := r.ensureExternalMachineConfigPool()

		if changed := r.setInProgressConditionToTargetMcpNotReady("worker-dpu", missingPoolErr); !changed {
			t.Fatal("expected missing target MCP condition to change")
		}
		condition := r.findInProgressCondition()
		if condition == nil {
			t.Fatal("expected InProgress condition")
		}
		if condition.Status != corev1.ConditionTrue || condition.Reason != targetMcpNotFoundReason {
			t.Fatalf("unexpected missing target MCP condition: %#v", condition)
		}
		if condition.Message != `Target MachineConfigPool "worker-dpu" was not found` {
			t.Fatalf("unexpected condition message: %q", condition.Message)
		}
		if changed := r.setInProgressConditionToTargetMcpNotReady("worker-dpu", missingPoolErr); changed {
			t.Fatal("expected an unchanged condition to avoid another status update")
		}

		if changed := r.recoverFromTargetMcpFailure(); !changed {
			t.Fatal("expected target MCP recovery to change the condition")
		}
		if condition.Status != corev1.ConditionTrue || condition.Reason != "Installing" {
			t.Fatalf("unexpected recovered condition: %#v", condition)
		}
	})

	t.Run("replaces not found when the pool returns but is not ready", func(t *testing.T) {
		missingPoolReconciler := externalMachineConfigPoolReconciler(t)
		missingPoolErr := missingPoolReconciler.ensureExternalMachineConfigPool()
		if changed := missingPoolReconciler.setInProgressConditionToTargetMcpNotReady("worker-dpu", missingPoolErr); !changed {
			t.Fatal("expected missing target MCP condition to change")
		}

		pausedMcp := externalMachineConfigPool("worker-dpu")
		pausedMcp.Spec.Paused = true
		pausedPoolReconciler := externalMachineConfigPoolReconciler(t, pausedMcp)
		pausedPoolReconciler.kataConfig.Status = missingPoolReconciler.kataConfig.Status
		pausedPoolErr := pausedPoolReconciler.ensureExternalMachineConfigPool()
		if changed := pausedPoolReconciler.setInProgressConditionToTargetMcpNotReady("worker-dpu", pausedPoolErr); !changed {
			t.Fatal("expected target MCP condition to change from not found to not ready")
		}

		condition := pausedPoolReconciler.findInProgressCondition()
		if condition.Reason != targetMcpNotReadyReason || !strings.Contains(condition.Message, "paused") {
			t.Fatalf("unexpected not-ready condition: %#v", condition)
		}
	})

	t.Run("persists recovery when the pool returns", func(t *testing.T) {
		t.Setenv("SANDBOXED_CONTAINERS_EXTENSION", "sandboxed-containers")
		mcp := externalMachineConfigPool("worker-dpu")
		r := externalMachineConfigPoolReconciler(t, mcp)
		if err := r.Create(context.Background(), r.kataConfig); err != nil {
			t.Fatal(err)
		}

		missingPoolErr := externalMachineConfigPoolReconciler(t).ensureExternalMachineConfigPool()
		r.setInProgressConditionToTargetMcpNotReady("worker-dpu", missingPoolErr)
		if err := r.Status().Update(context.Background(), r.kataConfig); err != nil {
			t.Fatal(err)
		}

		if _, err := r.processKataConfigInstallRequest(); err != nil {
			t.Fatalf("unexpected installation error after target MCP recovery: %v", err)
		}

		stored := &kataconfigurationv1.KataConfig{}
		if err := r.Get(context.Background(), types.NamespacedName{Name: r.kataConfig.Name}, stored); err != nil {
			t.Fatal(err)
		}
		if len(stored.Status.Conditions) != 1 || stored.Status.Conditions[0].Reason != "Installing" {
			t.Fatalf("unexpected persisted recovery condition: %#v", stored.Status.Conditions)
		}
	})

	t.Run("does not overwrite an unrelated failure", func(t *testing.T) {
		r := externalMachineConfigPoolReconciler(t)
		r.kataConfig.Status.Conditions = []kataconfigurationv1.KataConfigCondition{{
			Type:   kataconfigurationv1.KataConfigInProgress,
			Status: corev1.ConditionTrue,
			Reason: "Failed",
		}}

		if changed := r.recoverFromTargetMcpFailure(); changed {
			t.Fatal("expected unrelated condition to remain unchanged")
		}
		if condition := r.findInProgressCondition(); condition.Reason != "Failed" {
			t.Fatalf("unexpected condition after recovery check: %#v", condition)
		}
	})
}

func TestExternalMachineConfigPoolLogLevelSelector(t *testing.T) {
	mcp := externalMachineConfigPool("worker-dpu")
	r := externalMachineConfigPoolReconciler(t, mcp)
	mcp.Labels[externalMCPSelectorLabel] = r.kataConfig.Name

	if err := r.processLogLevel("debug"); err != nil {
		t.Fatalf("unexpected error processing log level: %v", err)
	}

	crc := &mcfgv1.ContainerRuntimeConfig{}
	if err := r.Get(context.Background(), types.NamespacedName{Name: container_runtime_config_name}, crc); err != nil {
		t.Fatal(err)
	}
	selector, err := metav1.LabelSelectorAsSelector(crc.Spec.MachineConfigPoolSelector)
	if err != nil {
		t.Fatalf("invalid ContainerRuntimeConfig selector: %v", err)
	}
	if !selector.Matches(labels.Set(mcp.Labels)) {
		t.Fatalf("ContainerRuntimeConfig selector %q does not match worker-dpu labels", selector)
	}
}

func TestExternalMachineConfigPoolStatusAndWatch(t *testing.T) {
	mcp := externalMachineConfigPool("worker-dpu")
	mcp.Spec.Configuration.Name = "rendered-worker-dpu-kata"
	mcp.Spec.Configuration.Source = []corev1.ObjectReference{{Name: extension_mc_name}}
	r := externalMachineConfigPoolReconciler(t, mcp)
	node := &corev1.Node{
		ObjectMeta: metav1.ObjectMeta{
			Name: "dpu-worker-0",
			Labels: map[string]string{
				"node-role.kubernetes.io/worker":     "",
				"node-role.kubernetes.io/worker-dpu": "",
			},
			Annotations: map[string]string{
				"machineconfiguration.openshift.io/state":         NodeDone,
				"machineconfiguration.openshift.io/currentConfig": mcp.Spec.Configuration.Name,
			},
		},
	}

	if err := r.putNodeOnStatusList(node); err != nil {
		t.Fatalf("unexpected status error: %v", err)
	}
	if !reflect.DeepEqual(r.kataConfig.Status.KataNodes.Installed, []string{node.Name}) {
		t.Fatalf("expected node to be installed, got %#v", r.kataConfig.Status.KataNodes)
	}
	if !r.isMcpRelevant(mcp) {
		t.Fatal("expected worker-dpu MCP to be relevant to reconciliation")
	}
	if r.isMcpRelevant(&mcfgv1.MachineConfigPool{ObjectMeta: metav1.ObjectMeta{Name: "infra"}}) {
		t.Fatal("expected unrelated MCP not to be relevant to reconciliation")
	}
}

func TestExternalMachineConfigPoolRequiresConvergence(t *testing.T) {
	for _, test := range []struct {
		name                 string
		desiredConfiguration string
		statusConfiguration  string
		kataMachineConfig    string
		enabled              bool
		expected             bool
	}{
		{
			name:                 "desired config rendered but nodes not converged",
			desiredConfiguration: "rendered-worker-dpu-kata",
			statusConfiguration:  "rendered-worker-dpu-old",
			kataMachineConfig:    extension_mc_name,
			enabled:              true,
			expected:             false,
		},
		{
			name:                 "all nodes converged on extension config",
			desiredConfiguration: "rendered-worker-dpu-kata",
			statusConfiguration:  "rendered-worker-dpu-kata",
			kataMachineConfig:    extension_mc_name,
			enabled:              true,
			expected:             true,
		},
		{
			name:                 "all nodes converged on layered image config",
			desiredConfiguration: "rendered-worker-dpu-kata",
			statusConfiguration:  "rendered-worker-dpu-kata",
			kataMachineConfig:    image_mc_name,
			enabled:              true,
			expected:             true,
		},
		{
			name:                 "all nodes converged after Kata removal",
			desiredConfiguration: "rendered-worker-dpu-without-kata",
			statusConfiguration:  "rendered-worker-dpu-without-kata",
			enabled:              false,
			expected:             true,
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			mcp := externalMachineConfigPool("worker-dpu")
			mcp.Spec.Configuration.Name = test.desiredConfiguration
			if test.kataMachineConfig != "" {
				mcp.Spec.Configuration.Source = []corev1.ObjectReference{{Name: test.kataMachineConfig}}
			}
			mcp.Status = mcfgv1.MachineConfigPoolStatus{
				Configuration: mcfgv1.MachineConfigPoolStatusConfiguration{
					ObjectReference: corev1.ObjectReference{Name: test.statusConfiguration},
				},
				MachineCount:            1,
				UpdatedMachineCount:     1,
				ReadyMachineCount:       1,
				UnavailableMachineCount: 0,
				DegradedMachineCount:    0,
				Conditions: []mcfgv1.MachineConfigPoolCondition{{
					Type:   mcfgv1.MachineConfigPoolUpdated,
					Status: corev1.ConditionTrue,
				}},
			}
			r := externalMachineConfigPoolReconciler(t, mcp)

			actual, err := r.isMcpConvergedAtKataState(mcp.Name, test.enabled)
			if err != nil {
				t.Fatalf("unexpected convergence error: %v", err)
			}
			if actual != test.expected {
				t.Fatalf("expected convergence %t, got %t", test.expected, actual)
			}
		})
	}
}

func TestExternalMachineConfigPoolSpecChangeEnqueuesReconcile(t *testing.T) {
	oldMCP := externalMachineConfigPool("worker-dpu")
	newMCP := oldMCP.DeepCopy()
	newMCP.Spec.Paused = true
	r := externalMachineConfigPoolReconciler(t, oldMCP)
	queue := workqueue.NewTypedRateLimitingQueue(workqueue.DefaultTypedControllerRateLimiter[reconcile.Request]())
	defer queue.ShutDown()

	handler := McpEventHandler{reconciler: r}
	handler.Update(context.Background(), event.UpdateEvent{ObjectOld: oldMCP, ObjectNew: newMCP}, queue)

	if queue.Len() != 1 {
		t.Fatalf("expected one reconcile request, got %d", queue.Len())
	}
	request, shutdown := queue.Get()
	if shutdown {
		t.Fatal("queue shut down before returning reconcile request")
	}
	defer queue.Done(request)
	if request.Name != r.kataConfig.Name {
		t.Fatalf("expected reconcile request for %q, got %q", r.kataConfig.Name, request.Name)
	}
}
