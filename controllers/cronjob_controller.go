/*


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
	batchv1 "cron-job/api/v1"
	"fmt"
	"github.com/robfig/cron"
	kbatch "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/tools/reference"
	"sort"
	"time"

	"github.com/go-logr/logr"
	"k8s.io/apimachinery/pkg/runtime"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// CronJobReconciler reconciles a CronJob object
type CronJobReconciler struct {
	client.Client
	Log    logr.Logger
	Scheme *runtime.Scheme
	Clock
}

type Clock interface {
	Now() time.Time
}

type realClock struct{}

func (_ realClock) Now() time.Time {
	return time.Now()
}

// +kubebuilder:rbac:groups=batch.tutorial.kubebuilder.io,resources=cronjobs,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=batch.tutorial.kubebuilder.io,resources=cronjobs/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=batch,resources=jobs,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=batch,resources=jobs/status,verbs=get

var (
	scheduledTimeAnnotation = "batch.tutorial.kubebuilder.io/scheduled-at"
)

var (
	jobOwnerKey = ".metadata.controller"
	apiGVStr    = batchv1.GroupVersion.String()
)

func (r *CronJobReconciler) Reconcile(req ctrl.Request) (ctrl.Result, error) {
	ctx := context.Background()
	log := r.Log.WithValues("cronjob", req.NamespacedName)

	// your logic here

	// 根据名称加载定时任务
	var cronJob batchv1.CronJob
	if err := r.Get(ctx, req.NamespacedName, &cronJob); err != nil {
		log.Error(err, "unable to fetch CronJob")
		//忽略掉 not-found 错误，它们不能通过重新排队修复（要等待新的通知）
		//在删除一个不存在的对象时，可能会报这个错误。
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}

	// 获取所有有效的job，并使用client.MatchingFields给查询到的数据加上索引
	// 用于加快后续的遍历有效job时的速率
	var childJobs kbatch.JobList
	//var childJobs batchv1.CronJobList
	if err := r.List(ctx, &childJobs, client.InNamespace(req.Namespace), client.MatchingFields{jobOwnerKey: req.Name}); err != nil {
		log.Error(err, "unable to  list child jobs")
		return ctrl.Result{}, nil
	}
	// 找出所有有效的job
	var activeJobs []*kbatch.Job
	var successfulJobs []*kbatch.Job
	var failedJobs []*kbatch.Job
	var mostRecentTime *time.Time // 最近一次运行状态

	// 判断一个任务是否完成
	isJobFinished := func(job *kbatch.Job) (bool, kbatch.JobConditionType) {
		for _, c := range job.Status.Conditions {
			if (c.Type == kbatch.JobFailed || c.Type == kbatch.JobComplete) && c.Status == corev1.ConditionTrue {
				return true, c.Type
			}
		}
		return false, ""
	}

	// 使用函数来提取创建job时Annotations中排定的执行时间
	getScheduledTimeForJob := func(job *kbatch.Job) (*time.Time, error) {
		timeRaw := job.Annotations[scheduledTimeAnnotation]
		if len(timeRaw) == 0 {
			return nil, nil
		}

		timeParsed, err := time.Parse(time.RFC3339, timeRaw)
		if err != nil {
			return nil, err
		}
		return &timeParsed, nil
	}

	for _, job := range childJobs.Items {
		_, finishedType := isJobFinished(&job)
		switch finishedType {
		case "":
			activeJobs = append(activeJobs, &job)
		case kbatch.JobFailed:
			failedJobs = append(failedJobs, &job)
		case kbatch.JobComplete:
			successfulJobs = append(successfulJobs, &job)
		}

		// 将启动时间放在annotation里面，当job生效时可以从中读取
		scheduledTimeForJob, err := getScheduledTimeForJob(&job)
		if err != nil {
			log.Error(err, "unable to parse Schedule time for child job", "job", &job)
			continue
		}

		if scheduledTimeForJob != nil {
			if mostRecentTime == nil {
				mostRecentTime = scheduledTimeForJob
			} else if mostRecentTime.Before(*scheduledTimeForJob) {
				mostRecentTime = scheduledTimeForJob
			}
		}
	}

	if mostRecentTime != nil {
		cronJob.Status.LastScheduleTime = &metav1.Time{Time: *mostRecentTime}
	} else {
		cronJob.Status.LastScheduleTime = nil
	}
	cronJob.Status.Active = nil
	for _, activeJob := range activeJobs {
		jobRef, err := reference.GetReference(r.Scheme, activeJob)
		if err != nil {
			log.Error(err, "unable to make reference job", "job", activeJob)
			continue
		}
		cronJob.Status.Active = append(cronJob.Status.Active, *jobRef)
	}

	// 记录我们观察到的 job 数量。为便于调试，略微提高日志级别
	log.V(1).Info("job count", "active jobs", len(activeJobs), "successful jobs", len(successfulJobs), "failed jobs", len(failedJobs))

	// 使用收集到日期信息来更新 CRD 状态。和之前类似，通过 client 来完成操作。 针对 status 这一子资源，我们可以使用Status部分的Update方法。
	//status 子资源会忽略掉对 spec 的变更。这与其它更新操作的发生冲突的风险更小， 而且实现了权限分离。
	if err := r.Status().Update(ctx, &cronJob); err != nil {
		log.Error(err, "unable to update CronJob status")
		return ctrl.Result{}, err
	}

	// 3、保留历史版本，清理过旧job
	if cronJob.Spec.FailedJobHistoryLimit != nil {
		sort.Slice(failedJobs, func(i, j int) bool {
			if failedJobs[i].Status.StartTime == nil {
				return failedJobs[j].Status.StartTime != nil
			}
			return failedJobs[i].Status.StartTime.Before(failedJobs[j].Status.StartTime)
		})
	}
	// 遍历failJobs，对超出最大保存条数的索引值对应的job删除
	for i, job := range failedJobs {
		if int32(i) >= int32(len(failedJobs))-*cronJob.Spec.FailedJobHistoryLimit { // 大于跳出 不做删除操作
			break
		}
		if err := r.Delete(ctx, job, client.PropagationPolicy(metav1.DeletePropagationBackground)); client.IgnoreNotFound(err) != nil {
			log.Error(err, "unable to delete old failed job", "job", job)
		} else {
			log.V(0).Info("deleted old failed job", "job", job)
		}
	}
	// 同上，成功的job也同样搞一套
	if cronJob.Spec.SuccessfulJobHistoryLimit != nil {
		sort.Slice(successfulJobs, func(i, j int) bool {
			if successfulJobs[i].Status.StartTime == nil {
				return successfulJobs[j].Status.StartTime != nil
			}
			return successfulJobs[i].Status.StartTime.Before(successfulJobs[j].Status.StartTime)
		})
	}
	for i, job := range successfulJobs {
		if int32(i) >= int32(len(successfulJobs))-*cronJob.Spec.SuccessfulJobHistoryLimit {
			break
		}
		if err := r.Delete(ctx, job, client.PropagationPolicy(metav1.DeletePropagationBackground)); err != nil {
			log.Error(err, "unable to delete old successful job", "job", job)
		} else {
			log.V(0).Info("deleted old successful job", "job", job)
		}
	}

	// 4、检查任务是否被挂起
	if cronJob.Spec.Suspend != nil && *cronJob.Spec.Suspend {
		log.V(1).Info("cron job suspended, skipping")
		return ctrl.Result{}, nil
	}

	// 5、计算下一job被执行时间
	getNextSchedule := func(job *batchv1.CronJob, now time.Time) (lastMissed time.Time, next time.Time, err error) {
		sched, err := cron.ParseStandard(cronJob.Spec.Schedule)
		if err != nil {
			return time.Time{}, time.Time{}, err
		}

		// 出于优化的目的，我们可以使用点技巧。从上一次观察到的执行时间开始执行，
		// 这个执行时间可以被在这里被读取。但是意义不大，因为我们刚更新了这个值
		var earliestTime time.Time
		if cronJob.Status.LastScheduleTime != nil {
			earliestTime = cronJob.Status.LastScheduleTime.Time
		} else {
			earliestTime = cronJob.ObjectMeta.CreationTimestamp.Time
		}
		if cronJob.Spec.StartingDeadlineSeconds != nil {
			// 如果开始执行时间超过了截止时间，不再执行
			schedulingDeadline := now.Add(-time.Second * time.Duration(*cronJob.Spec.StartingDeadlineSeconds))
			if schedulingDeadline.After(earliestTime) {
				earliestTime = schedulingDeadline
			}
		}
		if earliestTime.After(now) {
			return time.Time{}, sched.Next(now), nil
		}

		start := 0
		for t := sched.Next(earliestTime); !t.After(now); t = sched.Next(t) {
			lastMissed = t
			start++
			if start > 100 {
				return time.Time{}, time.Time{}, fmt.Errorf("Too many missed start times(> 100). set decrease .spec.startingDeadlineSeconds or  check clock shew")
			}
		}
		return lastMissed, sched.Next(now), nil
	}

	missRun, nextRun, err := getNextSchedule(&cronJob, r.Now())
	if err != nil {
		log.Error(err, "unable to figure out cronjob schedule")
		return ctrl.Result{}, err
	}
	scheduleResult := ctrl.Result{RequeueAfter: nextRun.Sub(r.Now())} // 保存以便别处使用
	log = log.WithValues("now", r.Now(), "next run", nextRun)

	if missRun.IsZero() {
		log.V(1).Info("no upcoming scheduled times, sleeping unit next schedule")
		return scheduleResult, nil
	}

	// 确保错过的任务现在还没过执行deadline
	log = log.WithValues("current run", missRun)
	tooLate := false
	if cronJob.Spec.StartingDeadlineSeconds != nil {
		tooLate = missRun.Add(time.Duration(*cronJob.Spec.StartingDeadlineSeconds) * time.Second).After(r.Now())
	}
	if tooLate { // 超出deadline，休眠至下次调度
		log.V(1).Info("missed starting deadline for last one, sleeping until next")
		return scheduleResult, nil
	}

	// 经过上述的筛选，下面我们进行定时任务的调度，有三种调度方法：1、直接运行，不考虑其他job 2、直接覆盖现在运行的job 3、等待当前的job运行完成后，再进行调度运行
	if cronJob.Spec.ConcurrencyPolicy == batchv1.ForbidConcurrent && len(activeJobs) > 0 { // 不允许并行调度
		log.V(1).Info("Concurrency policy blocks concurrent runs, skipping scheduling", "nums active", len(activeJobs))
		return scheduleResult, nil
	}
	// 覆盖运行现有job
	if cronJob.Spec.ConcurrencyPolicy == batchv1.ReplaceConcurrent {
		for _, job := range activeJobs {
			if err := r.Delete(ctx, job, client.PropagationPolicy(metav1.DeletePropagationBackground)); err != client.IgnoreNotFound(err) {
				log.Error(err, "unabel to delete active job", "job", job)
				return ctrl.Result{}, err
			}
		}
	}
	// 确定如何处理现有job，开始来创建预期的job
	// 基于 CronJob 模版构建 job，从模板复制 spec 及对象的元信息。
	// 然后在注解中设置执行时间，这样我们可以在每次的调谐中获取起作为“上一次执行时间”
	// 最后，还需要设置 owner reference字段。当我们删除 CronJob 时，Kubernetes 垃圾收集器会根据
	// 这个字段对应的job进行对应垃圾回收。同时，当某个job状态发生变更（创建，删除，完成）时，
	//controller-runtime 可以根据这个字段识别出要对那个 CronJob 进行调谐。
	constructJobForCronJob := func(cronJob *batchv1.CronJob, scheduledTime time.Time) (*kbatch.Job, error) {
		// job 名称带上执行时间以确保唯一性，避免排定执行时间的 job 创建两次
		name := fmt.Sprintf("%s-%d", cronJob.Name, scheduledTime.Unix())
		job := &kbatch.Job{
			ObjectMeta: metav1.ObjectMeta{
				Labels:      make(map[string]string),
				Annotations: make(map[string]string),
				Name:        name,
				Namespace:   cronJob.Namespace,
			},
			Spec: *cronJob.Spec.JobTemplate.Spec.DeepCopy(),
		}
		for k, v := range cronJob.Spec.JobTemplate.Annotations {
			job.Annotations[k] = v
		}
		job.Annotations[scheduledTimeAnnotation] = scheduledTime.Format(time.RFC3339)
		for k, v := range cronJob.Spec.JobTemplate.Labels {
			job.Labels[k] = v
		}
		if err := ctrl.SetControllerReference(cronJob, job, r.Scheme); err != nil {
			return nil, err
		}
		return job, nil
	}

	// 构建 job
	job, err := constructJobForCronJob(&cronJob, missRun)
	if err != nil {
		log.Error(err, "unable to construct job from template")
		// job 的 spec 没有变更，无需重新排队，不返回错误
		return scheduleResult, nil
	}

	if err := r.Create(ctx, job); err != nil {
		log.Error(err, "unable to create job for cronJob", "job", job)
		return ctrl.Result{}, err
	}

	log.V(1).Info("created job for Cronjob run", "job", job)

	//return ctrl.Result{}, nil
	return scheduleResult, nil // 返回调度结果
}

func (r *CronJobReconciler) SetupWithManager(mgr ctrl.Manager) error {
	// 给定一个真实时钟
	if r.Clock == nil {
		r.Clock = realClock{}
	}

	// 确定如何索引
	indexFunc := func(rawObj runtime.Object) []string {
		//获取 job 对象，提取 owner
		job := rawObj.(*kbatch.Job)
		owner := metav1.GetControllerOf(job)
		if owner == nil {
			return nil
		}
		// 确保 owner 是个CronJob
		if owner.APIVersion != apiGVStr || owner.Kind != "CronJob" {
			return nil
		}

		// 是 CronJob，返回
		return []string{owner.Name}
	}

	if err := mgr.GetFieldIndexer().IndexField(&kbatch.Job{}, jobOwnerKey, indexFunc); err != nil {
		return err
	}

	return ctrl.NewControllerManagedBy(mgr).
		For(&batchv1.CronJob{}).
		Owns(&kbatch.Job{}).
		Complete(r)
}
