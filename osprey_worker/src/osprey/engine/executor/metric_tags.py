from typing import List, Sequence

# `host:none` makes the Datadog Agent report the series under host `none`, so the node's host tags (`kube_node` and
# the cloud instance tags) never attach; the other entries pin those keys to `none`.
NODE_OVERRIDE_TAGS = ('host:none', 'kube_node:none', 'instance-id:none', 'internal-hostname:none', 'name:none')


def failure_metric_tags(tags: Sequence[str]) -> List[str]:
    """Failed UDF executions keep the Agent's host and its node tags, so errors can be grouped by node."""
    return [tag for tag in tags if tag not in NODE_OVERRIDE_TAGS]
