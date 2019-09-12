import copy
import cvxpy as cp
import numpy as np
import random

import job_id_pair

class Policy:

    def __init__(self):
        self._name = None

    @property
    def name(self):
        return self._name

    def flatten(self, d, cluster_spec):
        """Converts a 2-level dict to a NumPy array."""

        job_ids = sorted(list(d.keys()))
        if len(job_ids) == 0:
            return None, None
        worker_types = sorted(list(d[job_ids[0]].keys()))
        self._num_workers = \
            [cluster_spec[worker_type] for worker_type in worker_types]
        if len(worker_types) == 0:
            return None, None
        m = []
        for job_id in job_ids:
            m_row = []
            for worker_type in worker_types:
                m_row.append(d[job_id][worker_type])
            m.append(m_row)
        return np.array(m), (job_ids, worker_types)

    def unflatten(self, m, index):
        """Converts a NumPy array to a 2-level dict."""

        (job_ids, worker_types) = index
        d = {}
        for i in range(len(job_ids)):
            d[job_ids[i]] = {}
            for j in range(len(worker_types)):
                d[job_ids[i]][worker_types[j]] = m[i][j]
        return d


class PolicyWithPacking(Policy):

    def flatten(self, d, cluster_spec, normalize=True,
                priority_weights=None):
        """
        Converts a 2-level dict to a NumPy array.

        Job ID combinations in the input dict are either a tuple or an integer.
        If a tuple, represents a combination run on a GPU concurrently.
        If an integer, represents a single job / application run on the
        GPU.

        Returns a list of each user's normalized throughput matrix, a list
        of masks (used for normalization in the linear program), and an
        index to reconstruct the allocation as a dict.
        """

        job_ids = sorted(list(d.keys()))
        if len(job_ids) == 0:
            return None, None, None
        worker_types = sorted(list(d[job_ids[0]].keys()))
        self._num_workers = \
            [cluster_spec[worker_type] for worker_type in worker_types]

        single_job_ids = set()
        sorted_single_job_ids = []
        for job_id in job_ids:
            if not job_id.is_pair():
                single_job_ids.add(job_id)
                sorted_single_job_ids.append(job_id)

        # Compute normalizing factor for each individual job, this normalizing
        # factor will be used to normalize throughputs for the same job in job
        # combinations as well.
        if normalize:
            normalizing_factors = {}
            for single_job_id in single_job_ids:
                normalizing_factor = 0.0
                for worker_type in worker_types:
                    normalizing_factor += d[single_job_id][worker_type]
                normalizing_factors[single_job_id] = normalizing_factor

        if len(worker_types) == 0:
            return None, None, None

        shape = (len(single_job_ids), len(job_ids), len(worker_types))
        all_m = np.zeros(shape, dtype=np.float32)
        masks = np.zeros(shape, dtype=np.float32)
        # Compute the throughput matrix and mask for each individual job.
        for i, single_job_id in enumerate(sorted_single_job_ids):
            # Each throughput matrix and mask has dimension
            # (num_app_combinations x num_worker_types).
            for j, job_id in enumerate(job_ids):
                for k, worker_type in enumerate(worker_types):
                    # If job ID of interest is not in this job_id_combination,
                    # mask and throughput should be 0.
                    # Otherwise, use the right throughput from the input dict.
                    if job_id in single_job_ids:
                        if job_id == single_job_id:
                            all_m[i][j][k] = d[job_id][worker_type]
                            masks[i][j][k] = 1.0
                    else:
                        if single_job_id.overlaps_with(job_id):
                            # Find the index of the job of interest in the job
                            # combination tuple.
                            index = job_id.as_tuple().index(single_job_id[0])
                            throughputs = d[job_id][worker_type]
                            all_m[i][j][k] = d[job_id][worker_type][index]
                            masks[i][j][k] = 1.0
            # Normalize.
            if normalize:
                all_m[i] /= normalizing_factors[single_job_id]
                if priority_weights is not None:
                    all_m[i] /= priority_weights[single_job_id]
        return all_m, masks, (job_ids, single_job_ids, worker_types)

    def unflatten(self, m, index):
        """Converts a NumPy array to a 2-level dict."""

        (job_id_combinations, single_job_ids, worker_types) = index
        d = {}
        for i in range(len(job_id_combinations)):
            d[job_id_combinations[i]] = {}
            for j in range(len(worker_types)):
                d[job_id_combinations[i]][worker_types[j]] = m[i][j]
        return d


class MaxMinFairnessPolicy(Policy):

    def __init__(self):
        self._name = 'MaxMinFairness'

    def get_allocation(self, unflattened_throughputs,
                       unflattened_priority_weights, cluster_spec):
        throughputs, index = super().flatten(unflattened_throughputs,
                                             cluster_spec)
        if throughputs is None: return None
        (m, n) = throughputs.shape
        (job_ids, worker_types) = index

        priority_weights = np.array(
            [unflattened_priority_weights[job_id]
             for job_id in job_ids]).reshape((m, 1))
        priority_weights /= sum(priority_weights)
        num_workers = [cluster_spec[worker_type] for worker_type in worker_types]
        allocation = np.repeat(priority_weights, n, axis=1)

        # Give each user 1/m of the cluster (num_workers[i] workers
        # available to each user i).
        for i in range(n):
            allocation[:, i] *= num_workers[i]

        # Normalization to make sure each row in the allocation has a sum
        # <= 1, and each column in the allocation has a sum <= num_workers
        # of that type.
        for i in range(m):
            allocation[i] = allocation[i] / max(1.0, np.sum(allocation[i]))
        return super().unflatten(allocation, index)


class MaxMinFairnessPolicyWithPerf(Policy):

    def __init__(self):
        self._name = 'MaxMinFairness_Perf'

    def get_allocation(self, unflattened_throughputs,
                       unflattened_priority_weights, cluster_spec):
        throughputs, index = super().flatten(unflattened_throughputs,
                                             cluster_spec)
        if throughputs is None: return None
        (m, n) = throughputs.shape
        (job_ids, worker_types) = index

        priority_weights = np.array(
            [1. / unflattened_priority_weights[job_id]
             for job_id in job_ids])
        scale = 1.0 / throughputs.sum(axis=1)
        throughputs = throughputs * scale.reshape(m, 1)
        throughputs = throughputs * priority_weights.reshape((m, 1))

        x = cp.Variable(throughputs.shape)
        objective = cp.Maximize(cp.min(cp.sum(cp.multiply(throughputs, x),
                                              axis=1)))
        constraints = [
            x >= 0,
            cp.sum(x, axis=0) <= self._num_workers,
            cp.sum(x, axis=1) <= 1,
        ]
        cvxprob = cp.Problem(objective, constraints)
        result = cvxprob.solve(solver='SCS')

        if cvxprob.status != "optimal":
            print('WARNING: Allocation returned by policy not optimal!')

        return super().unflatten(x.value.clip(min=0.0).clip(max=1.0), index)


class MaxMinFairnessPolicyWithPacking(PolicyWithPacking):

    def __init__(self):
        self._name = 'MaxMinFairness_Packing'

    def get_allocation(self, unflattened_throughputs,
                       unflattened_priority_weights, cluster_spec):
        all_throughputs, masks, index = self.flatten(unflattened_throughputs,
                                                     cluster_spec,
                                                     unflattened_priority_weights)
        if all_throughputs is None or len(all_throughputs) == 0: return None
        x = cp.Variable(all_throughputs[0].shape)
        objective_terms = []
        for i in range(len(all_throughputs)):
            objective_terms.append(cp.sum(cp.multiply(all_throughputs[i], x)))
        if len(objective_terms) == 1:
            objective = cp.Maximize(objective_terms[0])
        else:
            objective = cp.Maximize(cp.minimum(*objective_terms))
        constraints = [
            x >= 0,
            cp.sum(x, axis=0) <= self._num_workers,
        ]
        for i in range(len(masks)):
            constraints.append(cp.sum(cp.multiply(x, masks[i])) <= 1)
        cvxprob = cp.Problem(objective, constraints)
        result = cvxprob.solve(solver='SCS')

        if cvxprob.status != "optimal":
            print('WARNING: Allocation returned by policy not optimal!')

        return self.unflatten(x.value.clip(min=0.0).clip(max=1.0), index)


class MaxSumThroughputPolicyWithPerf(Policy):

    def __init__(self):
        self._name = 'MaxSumThroughput_Perf'

    def get_allocation(self, unflattened_throughputs, cluster_spec):
        throughputs, index = super().flatten(unflattened_throughputs,
                                             cluster_spec)
        if throughputs is None: return None
        (m, n) = throughputs.shape
        scale = 1.0 / throughputs.sum(axis=1)
        throughputs = throughputs * scale.reshape(m, 1)

        x = cp.Variable(throughputs.shape)
        objective = cp.Maximize(cp.sum(cp.multiply(throughputs, x)))
        constraints = [
            x >= 0,
            cp.sum(x, axis=0) <= self._num_workers,
            cp.sum(x, axis=1) <= 1,
        ]
        cvxprob = cp.Problem(objective, constraints)
        result = cvxprob.solve(solver='SCS')

        if cvxprob.status != "optimal":
            print('WARNING: Allocation returned by policy not optimal!')

        return super().unflatten(x.value.clip(min=0.0).clip(max=1.0), index)


class MaxSumThroughputPolicyWithPacking(PolicyWithPacking):

    def __init__(self):
        self._name = 'MaxSumThroughput_Packing'

    def get_allocation(self, unflattened_throughputs, cluster_spec):
        all_throughputs, masks, index = self.flatten(unflattened_throughputs,
                                                     cluster_spec)
        if all_throughputs is None or len(all_throughputs) == 0: return None
        x = cp.Variable(all_throughputs[0].shape)
        objective_terms = []
        for i in range(len(all_throughputs)):
            objective_terms.append(cp.sum(cp.multiply(all_throughputs[i], x)))
        objective = cp.Maximize(sum(objective_terms))
        constraints = [
            x >= 0,
            cp.sum(x, axis=0) <= self._num_workers,
        ]
        for i in range(len(masks)):
            constraints.append(cp.sum(cp.multiply(x, masks[i])) <= 1)
        cvxprob = cp.Problem(objective, constraints)
        result = cvxprob.solve(solver='SCS')

        if cvxprob.status != "optimal":
            print('WARNING: Allocation returned by policy not optimal!')

        return self.unflatten(x.value.clip(min=0.0).clip(max=1.0), index)


class MinTotalDurationPolicy(Policy):

    def __init__(self):
        self._name = 'MinTotalDuration'

    def get_allocation_helper(self, throughputs, T):
        x = cp.Variable(throughputs.shape)
        objective = cp.Maximize(1)
        constraints = [
            x >= 0,
            cp.sum(x, axis=0) <= self._num_workers,
            cp.sum(x, axis=1) <= 1,
            cp.sum(cp.multiply(throughputs, x), axis=1) >= (
                self._num_steps_remaining / T),
        ]
        cvxprob = cp.Problem(objective, constraints)
        result = cvxprob.solve(solver='SCS')

        return cvxprob.status, x

    def get_allocation(self, unflattened_throughputs, num_steps_remaining,
                       cluster_spec):
        throughputs, index = super().flatten(unflattened_throughputs,
                                                        cluster_spec)
        if index is None: return None
        (job_ids, _) = index
        self._num_steps_remaining = np.array([num_steps_remaining[job_id]
                                              for job_id in job_ids])
        if throughputs is None: return None

        # Units are in seconds.
        max_T = 1000000.
        min_T = 100.
        last_max_T = max_T
        status = None
        last_feasible_x = None
        while last_feasible_x is None:
            # Binary search for the smallest T that gives an optimal solution.
            while (1.05 * min_T) < max_T:  # TODO: Can tweak the 1.05 in this loop.
                T = (min_T + max_T) / 2.
                status, x = self.get_allocation_helper(throughputs, T)
                if status == "optimal":
                    last_feasible_x = x
                    max_T = T
                else:
                    min_T = T
            max_T = last_max_T * 10.
            min_T = last_max_T
            last_max_T *= 10

        assert(last_feasible_x is not None)
        return super().unflatten(
            last_feasible_x.value.clip(min=0.0).clip(max=1.0), index)


class MinTotalDurationPolicyWithPacking(PolicyWithPacking):

    def __init__(self):
        self._name = 'MinTotalDuration_Packing'

    def get_allocation_helper(self, all_throughputs, masks, job_ids,
                              single_job_ids, T):
        x = cp.Variable(all_throughputs[0].shape)
        objective = cp.Maximize(1)
        constraints = [
            x >= 0,
            cp.sum(x, axis=0) <= self._num_workers,
        ]
        for mask in masks:
            # Every job cannot receive a total time share sum greater than 1.0.
            constraints.append(cp.sum(cp.multiply(x, mask)) <= 1)
        for throughputs, num_steps_remaining in zip(all_throughputs,
                                                    self._num_steps_remaining):
            # Ensure that every job satisfies its throughput constraint.
            constraints.append(cp.sum(cp.multiply(throughputs, x)) >=
                (num_steps_remaining / T))
        cvxprob = cp.Problem(objective, constraints)
        result = cvxprob.solve(solver='SCS')

        return cvxprob.status, x

    def get_allocation(self, unflattened_throughputs, num_steps_remaining,
                       cluster_spec):
        all_throughputs, masks, index = super().flatten(unflattened_throughputs,
                                                        cluster_spec, normalize=False)
        if all_throughputs is None or len(all_throughputs) == 0: return None
        if index is None: return None
        (job_ids, single_job_ids, _) = index
        self._num_steps_remaining = [num_steps_remaining[single_job_id]
                                     for single_job_id in single_job_ids]

        # Units are in seconds.
        max_T = 1000000.
        min_T = 100.
        last_max_T = max_T
        status = None
        last_feasible_x = None
        while last_feasible_x is None:
            # Binary search for the smallest T that gives an optimal solution.
            while (1.05 * min_T) < max_T:  # TODO: Can tweak the 1.05 in this loop.
                T = (min_T + max_T) / 2.
                status, x = self.get_allocation_helper(all_throughputs, masks,
                                                       job_ids, single_job_ids, T)
                if status == "optimal":
                    last_feasible_x = x
                    max_T = T
                else:
                    min_T = T
            max_T = last_max_T * 10.
            min_T = last_max_T
            last_max_T *= 10

        assert(last_feasible_x is not None)
        return super().unflatten(
            last_feasible_x.value.clip(min=0.0).clip(max=1.0), index)


class FIFOPolicy(Policy):
    def __init__(self, mode='base', seed=None, packing_threshold=1.5):
        self._name = 'FIFO'
        self._mode = mode
        self._allocation = {}
        self._scale_factors = {}
        if mode == 'base':
            self._rng = random.Random()
            if seed is not None:
                self._rng.seed(seed)
        elif mode == 'packing':
            self._packing_threshold = packing_threshold

    def _pack(self, queue, throughputs, scale_factors):
        while len(queue) > 0:
            # Only make a packing decision if combined normalized
            # throughput would provide a signficant gain.
            max_packed_throughput = self._packing_threshold
            job_id_to_pack_with = None
            job_id_to_schedule = queue.pop(0)

            # Find the already scheduled job with which the next job on
            # the queue will pack best with.
            for scheduled_job_id in self._allocation:
                assert scheduled_job_id != job_id_to_schedule
                assert scheduled_job_id in throughputs
                if scheduled_job_id.is_pair():
                    continue
                if (scale_factors[scheduled_job_id] !=\
                        scale_factors[job_id_to_schedule]):
                    continue
                worker_type = self._allocation[scheduled_job_id]
                merged_job_id = \
                        job_id_pair.JobIdPair(scheduled_job_id[0],
                                              job_id_to_schedule[0])
                packed_throughput = throughputs[merged_job_id][worker_type]
                normalized_packed_throughput = 0.0
                for i, single_job_id in enumerate(merged_job_id.singletons()):
                    if packed_throughput[i] <= 0.0:
                        continue
                    isolated_throughput = \
                            throughputs[single_job_id][worker_type]
                    normalized_packed_throughput += \
                            packed_throughput[i] / isolated_throughput
                if normalized_packed_throughput > max_packed_throughput:
                    max_packed_throughput = normalized_packed_throughput
                    job_id_to_pack_with = scheduled_job_id
            if job_id_to_pack_with is None:
                # Terminate when we cannot find a job to pack with.
                # This respects the FIFO property of no jobs being able
                # to jump ahead in the queue.
                break
            else:
                # Transfer the allocation for the single job to the
                # packed job.
                self._output = None
                merged_job_id = \
                        job_id_pair.JobIdPair(job_id_to_pack_with[0],
                                              job_id_to_schedule[0])
                worker_type = self._allocation[job_id_to_pack_with]
                del self._allocation[job_id_to_pack_with]
                self._allocation[merged_job_id] = worker_type


    def get_allocation(self, throughputs, scale_factors, cluster_spec):
        available_workers = copy.deepcopy(cluster_spec)
        queue = []

        # Update the internal representation of scale_factors.
        for job_id in scale_factors:
            self._scale_factors[job_id] = scale_factors[job_id]

        # Reset the allocation when running in performance-aware mode.
        if self._mode != 'base':
            self._allocation = {}

        # Add all jobs that have not been allocated already to the queue.
        # Jobs should be added in order of arrival (i.e. according to Job ID).
        for job_id in sorted(list(throughputs.keys())):
            if job_id not in self._allocation and not job_id.is_pair():
                queue.append(job_id)

        # Find all completed jobs and schedule jobs off the queue to replace
        # them. Also determine how many workers are available.
        # NOTE: In performance-aware mode, this loop should be a no-op
        # because the allocation is reset.
        for scheduled_job_id in sorted(list(self._allocation.keys())):
            worker_type = self._allocation[scheduled_job_id]
            # Check if job has completed.
            if scheduled_job_id not in throughputs:
                # If only one job in a pair of co-located jobs completed, then
                # add the other job back to the queue.
                for single_job_id in scheduled_job_id.singletons():
                    if single_job_id in throughputs:
                        queue.append(single_job_id)
                        queue.sort()
                if len(queue) > 0:
                    job_id_to_schedule = queue.pop(0)
                    if (scale_factors[job_id_to_schedule] <=
                            available_workers[worker_type]):
                        worker_type = self._allocation[scheduled_job_id]
                        self._allocation[job_id_to_schedule] = worker_type
                        available_workers[worker_type] -= \
                            scale_factors[job_id_to_schedule]
                del self._allocation[scheduled_job_id]
                del self._scale_factors[scheduled_job_id]
            else:
                # Job has not completed, subtract its allocated workers
                # from available_workers.
                available_workers[worker_type] -= \
                    scale_factors[scheduled_job_id]

        # Find all available workers.
        available_worker_types = []
        for worker_type in available_workers:
            if available_workers[worker_type] > 0:
                available_worker_types.append(worker_type)
        available_worker_types.sort()

        # Allocate resources to as many jobs as possible.
        while len(queue) > 0 and len(available_worker_types) > 0:
            job_id_to_schedule = queue.pop(0)
            scale_factor = scale_factors[job_id_to_schedule]
            available_worker_types_with_scale_factor = []
            original_available_worker_types_mapping = []
            for i, worker_type in enumerate(available_worker_types):
                if available_workers[worker_type] >= scale_factor:
                    available_worker_types_with_scale_factor.append(worker_type)
                    original_available_worker_types_mapping.append(i)
            if len(available_worker_types_with_scale_factor) == 0:
                break
            if self._mode == 'base':
                worker_type_idx = self._rng.randrange(
                        len(available_worker_types_with_scale_factor))
            else:
                # Find the worker_type with best performance for this job.
                worker_type = None
                worker_type_idx = None
                max_throughput = -1
                for i, x in enumerate(available_worker_types_with_scale_factor):
                    throughput = throughputs[job_id_to_schedule][x]
                    if throughput > max_throughput:
                        max_throughput = throughput
                        worker_type = x
                        worker_type_idx = i
            self._allocation[job_id_to_schedule] = worker_type
            available_workers[worker_type] -= scale_factors[job_id_to_schedule]
            if available_workers[worker_type] == 0:
                worker_type_idx =\
                    original_available_worker_types_mapping[worker_type_idx]
                available_worker_types.pop(worker_type_idx)

        if self._mode == 'packing':
            self._pack(queue, throughputs, scale_factors)

        # Construct output allocation.
        final_allocation = {}
        for job_id in throughputs:
            final_allocation[job_id] = \
                    {worker_type: 0.0 for worker_type in cluster_spec}
        for job_id, worker_type in self._allocation.items():
            final_allocation[job_id][worker_type] = 1.0

        return final_allocation

class FIFOPolicyWithPerf(Policy):
    def __init__(self, packing=False):
        self._name = 'FIFO_Perf'
        self._packing = packing
        self._policy = FIFOPolicy(mode='perf')

    def get_allocation(self, throughputs, scale_factors, cluster_spec):
        return self._policy.get_allocation(throughputs, scale_factors,
                                           cluster_spec)

class FIFOPolicyWithPacking(PolicyWithPacking):
    def __init__(self, packing_threshold=1.5):
        self._name = 'FIFO_Packing'
        self._policy = FIFOPolicy(mode='packing',
                                  packing_threshold=packing_threshold)

    def get_allocation(self, throughputs, scale_factors, cluster_spec):
        return self._policy.get_allocation(throughputs, scale_factors,
                                           cluster_spec)
