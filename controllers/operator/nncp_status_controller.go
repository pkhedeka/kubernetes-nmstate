/*
Copyright The Kubernetes NMState Authors.


Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package controllers

import (
	"context"

	"github.com/go-logr/logr"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller"
	"sigs.k8s.io/controller-runtime/pkg/event"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	"sigs.k8s.io/controller-runtime/pkg/predicate"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
	"sigs.k8s.io/controller-runtime/pkg/source"

	nmstateapi "github.com/nmstate/kubernetes-nmstate/api/shared"
	nmstatev1beta1 "github.com/nmstate/kubernetes-nmstate/api/v1beta1"
	"github.com/nmstate/kubernetes-nmstate/pkg/policyconditions"
)

type NNCPStatusReconciler struct {
	Client    client.Client
	APIClient client.Reader
	Log       logr.Logger
}

func (r *NNCPStatusReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	r.Log.Info("Reconciling NNCP status", "policy", req.Name)
	if err := policyconditions.Update(ctx, r.Client, r.APIClient, req.NamespacedName); err != nil {
		r.Log.Error(err, "failed to update policy conditions", "policy", req.Name)
		return ctrl.Result{}, err
	}
	return ctrl.Result{}, nil
}

func (r *NNCPStatusReconciler) SetupWithManager(mgr ctrl.Manager) error {
	onEnactmentDeleted := predicate.TypedFuncs[*nmstatev1beta1.NodeNetworkConfigurationEnactment]{
		CreateFunc: func(event.TypedCreateEvent[*nmstatev1beta1.NodeNetworkConfigurationEnactment]) bool {
			return false
		},
		DeleteFunc: func(event.TypedDeleteEvent[*nmstatev1beta1.NodeNetworkConfigurationEnactment]) bool {
			return true
		},
		UpdateFunc: func(event.TypedUpdateEvent[*nmstatev1beta1.NodeNetworkConfigurationEnactment]) bool {
			return false
		},
		GenericFunc: func(event.TypedGenericEvent[*nmstatev1beta1.NodeNetworkConfigurationEnactment]) bool {
			return false
		},
	}

	enactmentToPolicy := handler.TypedEnqueueRequestsFromMapFunc[*nmstatev1beta1.NodeNetworkConfigurationEnactment](
		func(ctx context.Context, nnce *nmstatev1beta1.NodeNetworkConfigurationEnactment) []reconcile.Request {
			policyName, ok := nnce.GetLabels()[nmstateapi.EnactmentPolicyLabel]
			if !ok || policyName == "" {
				return nil
			}
			return []reconcile.Request{{NamespacedName: types.NamespacedName{Name: policyName}}}
		},
	)

	c, err := controller.New("nncp-status", mgr, controller.Options{Reconciler: r})
	if err != nil {
		return err
	}

	return c.Watch(
		source.Kind(
			mgr.GetCache(),
			&nmstatev1beta1.NodeNetworkConfigurationEnactment{},
			enactmentToPolicy,
			onEnactmentDeleted,
		),
	)
}
