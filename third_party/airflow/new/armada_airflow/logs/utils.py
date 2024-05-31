from typing import TYPE_CHECKING

from kubernetes.client import V1Pod, V1ContainerStatus

if TYPE_CHECKING:
    from kubernetes.client.models.v1_container_status import V1ContainerStatus
    from kubernetes.client.models.v1_pod import V1Pod


def get_container_status(pod: V1Pod, container_name: str) -> V1ContainerStatus | None:
    """Retrieve container status."""
    container_statuses = pod.status.container_statuses if pod and pod.status else None
    if container_statuses:
        # In general the variable container_statuses can store multiple items matching different containers.
        # The following generator expression yields all items that have name equal to the container_name.
        # The function next() here calls the generator to get only the first value. If there's nothing found
        # then None is returned.
        return next((x for x in container_statuses if x.name == container_name), None)
    return None


def container_is_running(pod: V1Pod, container_name: str) -> bool:
    """
    Examine V1Pod ``pod`` to determine whether ``container_name`` is running.

    If that container is present and running, returns True.  Returns False otherwise.
    """
    container_status = get_container_status(pod, container_name)
    if not container_status:
        return False
    return container_status.state.running is not None
